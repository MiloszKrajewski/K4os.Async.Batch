using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace K4os.Async.Batch
{
	internal static class Extensions
	{
		public static void Forget(this Task task)
		{
			task.ContinueWith(
				t => t.Exception, // clear exception so TPL stops complaining
				TaskContinuationOptions.NotOnRanToCompletion);
		}

		public static void CancelAndWait(this CancellationTokenSource token, Task task)
		{
			token.Cancel();
			
			try
			{
				task.GetAwaiter().GetResult();
			}
			catch (OperationCanceledException) when (token.IsCancellationRequested)
			{
				// ignore
			}
		}

		public static T Required<T>(this T argument, string argumentName) where T: class =>
			argument ?? throw new ArgumentNullException(argumentName);

		public static IList<T> TryDequeueMany<T>(
			this ConcurrentQueue<T> queue, int length = int.MaxValue)
		{
			var left = length;
			var result = default(IList<T>);
			while (left > 0 && queue.TryDequeue(out var item))
			{
				(result = result ?? new List<T>()).Add(item);
				left--;
			}

			return result;
		}

		public static TValue TryGetOrDefault<TKey, TValue>(
			this IDictionary<TKey, TValue> dictionary, TKey key,
			TValue defaultValue = default) =>
			dictionary.TryGetValue(key, out var result) ? result : defaultValue;

		public static void ForEach<T>(this IEnumerable<T> sequence, Action<T> action)
		{
			foreach (var item in sequence) action(item);
		}
	}
}
