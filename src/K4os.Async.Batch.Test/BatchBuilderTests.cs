using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestPlatform.Common.DataCollection;
using Xunit;

namespace K4os.Async.Batch.Test
{
	public class BatchBuilderTests
	{
		[Fact]
		public async Task AllRequestsAreMade()
		{
			var builder = BatchBuilder.Create<int, int, int>(
				r => r,
				r => r,
				Requester);

			async Task<int[]> Requester(int[] rl)
			{
				await Task.Delay(100);
				return rl;
			}

			var requests = Enumerable.Range(0, 1000).ToArray();
			var tasks = requests.Select(r => builder.Request(r));
			var responses = await Task.WhenAll(tasks);

			Assert.Equal(requests, responses);
		}

		[Fact]
		public async Task RequestsAreNotMadeConcurrently()
		{
			var counter = 0;
			var overlaps = 0;

			var builder = BatchBuilder.Create<int, int, int>(
				r => r,
				r => r,
				Requester,
				concurrency: 1);

			async Task<int[]> Requester(int[] rl)
			{
				if (Interlocked.Increment(ref counter) != 1)
					Interlocked.Increment(ref overlaps);
				await Task.Delay(100);
				Interlocked.Decrement(ref counter);
				return rl;
			}

			var requests = Enumerable.Range(0, 1000).ToArray();
			var tasks = requests.Select(r => builder.Request(r));
			var responses = await Task.WhenAll(tasks);

			Assert.Equal(requests, responses);
			Assert.Equal(0, overlaps);
		}

		[Fact]
		public async Task RequestsAreBatched()
		{
			var batches = 0;

			var builder = BatchBuilder.Create<int, int, int>(
				r => r,
				r => r,
				Requester,
				batchSize: 100);

			async Task<int[]> Requester(int[] rl)
			{
				Interlocked.Increment(ref batches);
				await Task.Delay(100);
				return rl;
			}

			var requests = Enumerable.Range(0, 1000).ToArray();
			var tasks = requests.Select(r => builder.Request(r));
			var responses = await Task.WhenAll(tasks);

			Assert.Equal(requests, responses);
			Assert.True(batches <= (requests.Length + 99) / 100 + 1);
		}

		[Fact]
		public async Task RequestIsMatchedWithResponse()
		{
			var builder = BatchBuilder.Create<int, int, string>(
				r => r + 1000,
				r => int.Parse(r) + 1000,
				Requester);

			async Task<string[]> Requester(int[] rl)
			{
				await Task.Delay(100);
				return rl.Select(r => r.ToString()).ToArray();
			}

			var requests = Enumerable.Range(0, 1000).ToArray();
			var tasks = requests.Select(r => builder.Request(r));
			var responses = await Task.WhenAll(tasks);

			Assert.Equal(requests.Select(r => r.ToString()), responses);
		}

		[Fact]
		public async Task MissingResponseThrowException()
		{
			var builder = BatchBuilder.Create<int, int>(
				r => r,
				Requester);

			async Task<int[]> Requester(int[] rl)
			{
				await Task.Delay(100);
				return rl.Where(r => r != 337).ToArray();
			}

			var requests = Enumerable.Range(0, 1000).ToArray();
			var tasks = requests.Select(r => builder.Request(r)).ToArray();

			foreach (var r in requests)
			{
				if (r != 337)
					Assert.Equal(r, await tasks[r]);
				else
					await Assert.ThrowsAsync<KeyNotFoundException>(() => tasks[r]);
			}
		}

		[Fact]
		public async Task WhenBatchFailsAllRequestFail()
		{
			var builder = BatchBuilder.Create<int, int>(
				r => r,
				Requester);

			async Task<int[]> Requester(int[] rl)
			{
				await Task.Yield();
				throw new ArgumentException("Not working!");
			}

			var requests = Enumerable.Range(0, 1000).ToArray();
			var tasks = requests.Select(r => builder.Request(r)).ToArray();

			foreach (var r in requests)
			{
				await Assert.ThrowsAsync<ArgumentException>(() => tasks[r]);
			}
		}
		
		[Fact]
		[SuppressMessage("ReSharper", "AccessToModifiedClosure")]
		public async Task BatchCanBeDelayed()
		{
			var handled = 0;
			int Read() => Interlocked.CompareExchange(ref handled, 0, 0);
			int Inc(int value) => Interlocked.Add(ref handled, value);
			
			var builder = BatchBuilder.Create<int, int>(
				r => r, Requester, 1000, TimeSpan.FromSeconds(1));

			Task<int[]> Requester(int[] rl)
			{
				Inc(rl.Length);
				return Task.FromResult(rl);
			}

			var tasks50 = Enumerable.Range(0, 50).Select(r => builder.Request(r)).ToArray();
			
			Assert.Equal(0, Read());

			await Task.Delay(500);
			
			var tasks75 = Enumerable.Range(50, 25).Select(r => builder.Request(r)).ToArray();
			
			Assert.Equal(0, Read());

			await Task.WhenAll(tasks50);
			await Task.WhenAll(tasks75);
			
			Assert.Equal(75, Read());
		}
		
		[Fact]
		[SuppressMessage("ReSharper", "AccessToModifiedClosure")]
		public async Task EvenDelayedBatchWillTriggerEarly()
		{
			var handled = 0;
			int Read() => Interlocked.CompareExchange(ref handled, 0, 0);
			int Inc(int value) => Interlocked.Add(ref handled, value);
			
			var builder = BatchBuilder.Create<int, int>(
				r => r, Requester, 20, TimeSpan.FromSeconds(1));

			Task<int[]> Requester(int[] rl)
			{
				Inc(rl.Length);
				return Task.FromResult(rl);
			}

			_ = Enumerable.Range(0, 50).Select(r => builder.Request(r)).ToArray();
			
			await Task.Delay(100);
			
			Assert.Equal(40, Read());

			await Task.Delay(1000);
			
			Assert.Equal(50, Read());
		}
	}
}
