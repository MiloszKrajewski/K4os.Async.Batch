using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace K4os.Async.Batch
{
	/// <summary>
	/// Batch builder factory.
	/// </summary>
	public static class BatchBuilder
	{
		internal static readonly UnboundedChannelOptions ChannelOptions = new() {
			SingleReader = true,
		};

		/// <summary>Creates new batch builder.</summary>
		/// <param name="requestId">Extracts request id from request.</param>
		/// <param name="responseId">Extracts request id from response (to match with request)</param>
		/// <param name="requestMany">Actual batch operation.</param>
		/// <param name="batchSize">Maximum batch size.</param>
		/// <param name="concurrency">Number of concurrent batch requests.</param>
		/// <typeparam name="TId">Type of request identifier.</typeparam>
		/// <typeparam name="TRequest">Type of request.</typeparam>
		/// <typeparam name="TResponse">Type of response.</typeparam>
		/// <returns>Batch builder.</returns>
		public static IBatchBuilder<TRequest, TResponse> Create<TId, TRequest, TResponse>(
			Func<TRequest, TId> requestId,
			Func<TResponse, TId> responseId,
			Func<TRequest[], Task<TResponse[]>> requestMany,
			int batchSize = 128,
			int concurrency = 1)
			where TId: notnull =>
			new BatchBuilder<TId, TRequest, TResponse>(
				requestId, responseId, requestMany, batchSize, concurrency);

		/// <summary>Creates new batch builder.</summary>
		/// <param name="responseId">Extracts request id from response (to match with request)</param>
		/// <param name="requestMany">Actual batch operation.</param>
		/// <param name="batchSize">Maximum batch size.</param>
		/// <param name="concurrency">Number of concurrent batch requests.</param>
		/// <typeparam name="TRequest">Type of request.</typeparam>
		/// <typeparam name="TResponse">Type of response.</typeparam>
		/// <returns>Batch builder.</returns>
		public static IBatchBuilder<TRequest, TResponse> Create<TRequest, TResponse>(
			Func<TResponse, TRequest> responseId,
			Func<TRequest[], Task<TResponse[]>> requestMany,
			int batchSize = 128,
			int concurrency = 1)
			where TRequest: notnull =>
			new BatchBuilder<TRequest, TRequest, TResponse>(
				r => r, responseId, requestMany, batchSize, concurrency);
	}

	/// <summary>Request batch builder.</summary>
	/// <typeparam name="TRequest">Type of request.</typeparam>
	/// <typeparam name="TResponse">Type of response.</typeparam>
	public interface IBatchBuilder<in TRequest, TResponse>: IDisposable
	{
		/// <summary>Execute a request inside a batch.</summary>
		/// <param name="request">A request.</param>
		/// <returns>Response.</returns>
		Task<TResponse> Request(TRequest request);
	}

	/// <summary>Request batch builder.</summary>
	/// <typeparam name="TId">Request id (used to match request with responses).</typeparam>
	/// <typeparam name="TRequest">Type of request.</typeparam>
	/// <typeparam name="TResponse">Type of response.</typeparam>
	public class BatchBuilder<TId, TRequest, TResponse>:
		IBatchBuilder<TRequest, TResponse>
		where TId: notnull
	{
		private readonly Func<TRequest, TId> _requestId;
		private readonly Func<TResponse, TId> _responseId;
		private readonly Func<TRequest[], Task<TResponse[]>> _requestMany;
		private readonly Channel<Mailbox> _channel;
		private readonly SemaphoreSlim _semaphore;
		private readonly Task _loop;

		/// <summary>
		/// Creates a batch builder.
		/// </summary>
		/// <param name="requestId">Extracts request id from request.</param>
		/// <param name="responseId">Extracts request id from response (to match with request)</param>
		/// <param name="requestMany">Actual batch operation.</param>
		/// <param name="batchSize">Maximum batch size.</param>
		/// <param name="concurrency">Number of concurrent batch requests.</param>
		public BatchBuilder(
			Func<TRequest, TId> requestId,
			Func<TResponse, TId> responseId,
			Func<TRequest[], Task<TResponse[]>> requestMany,
			int batchSize = 128,
			int concurrency = 1)
		{
			_requestId = requestId.Required(nameof(requestId));
			_responseId = responseId.Required(nameof(responseId));
			_requestMany = requestMany.Required(nameof(requestMany));
			_channel = Channel.CreateUnbounded<Mailbox>(BatchBuilder.ChannelOptions);
			_semaphore = new SemaphoreSlim(Math.Max(concurrency, 1));
			_loop = RequestLoop(batchSize);
		}

		/// <summary>Execute a request/call inside a batch.</summary>
		/// <param name="request">A request.</param>
		/// <returns>Response.</returns>
		public async Task<TResponse> Request(TRequest request)
		{
			var box = new Mailbox(request);
			await _channel.Writer.WriteAsync(box);
			return await box.Response.Task;
		}

		private async Task RequestLoop(int length)
		{
			while (!_channel.Reader.Completion.IsCompleted)
			{
				var requests = await _channel.Reader.ReadManyAsync(length);
				if (requests is null) continue;

				await _semaphore.WaitAsync();
				RequestMany(requests).Forget();
			}
		}

		private async Task RequestMany(ICollection<Mailbox> requests)
		{
			try
			{
				if (requests.Count <= 0) return;

				var map = requests
					.GroupBy(r => _requestId(r.Request))
					.ToDictionary(g => g.Key, g => g.ToArray());
				var keys = map.Keys.ToArray();

				try
				{
					var chosen = map
						.Select(kv => kv.Value[0].Request)
						.ToArray();
					var responses = await _requestMany(chosen);
					var handled = MarkAsComplete(responses, map);
					var missing = keys
						.Except(handled)
						.SelectMany(k => map.TryGetOrDefault(k).EmptyIfNull());
					MarkAsNotFound(missing);
				}
				catch (Exception e)
				{
					MarkAsFailed(requests, e);
				}
			}
			finally
			{
				_semaphore.Release();
			}
		}

		private static void MarkAsNotFound(IEnumerable<Mailbox> requests)
		{
			void NotFound(Mailbox box) =>
				box.Response.TrySetException(
					new KeyNotFoundException($"Missing response for {box.Request}"));

			requests.ForEach(NotFound);
		}

		private static void MarkAsFailed(IEnumerable<Mailbox> requests, Exception exception)
		{
			void Fail(Mailbox box) => box.Response.TrySetException(exception);
			requests.ForEach(Fail);
		}

		private IEnumerable<TId> MarkAsComplete(
			IEnumerable<TResponse> responses,
			IDictionary<TId, Mailbox[]> map)
		{
			foreach (var response in responses)
			{
				if (ReferenceEquals(response, null)) continue;

				void Complete(Mailbox box) => box.Response.TrySetResult(response);
				var key = _responseId(response);
				map.TryGetOrDefault(key)?.ForEach(Complete);
				yield return key;
			}
		}

		/// <inheritdoc/>
		public void Dispose()
		{
			_channel.Writer.Complete();
			_loop.Wait();
		}

		#region class Mailbox

		private class Mailbox
		{
			public TRequest Request { get; }
			public TaskCompletionSource<TResponse> Response { get; }

			public Mailbox(TRequest request)
			{
				Request = request;
				Response = new TaskCompletionSource<TResponse>(
					TaskCreationOptions.RunContinuationsAsynchronously);
			}
		}

		#endregion
	}
}
