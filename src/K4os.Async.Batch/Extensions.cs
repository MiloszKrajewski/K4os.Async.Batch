using System;
using System.Collections.Generic;
using System.Threading.Channels;
using System.Threading.Tasks;

#if NETSTANDARD2_1_OR_GREATER || NET5_0_OR_GREATER
using System.Diagnostics.CodeAnalysis;
#endif

namespace K4os.Async.Batch
{
	internal static class Extensions
	{
		public static async Task<List<T>?> ReadManyAsync<T>(
			this ChannelReader<T> reader, int length = int.MaxValue)
		{
			var ready = await reader.WaitToReadAsync();
			if (!ready) return null;

			var list = default(List<T>);
			Drain(reader, ref list, ref length);

			return list;
		}

		public static async Task ReadManyMoreAsync<T>(
			this ChannelReader<T> reader, List<T> list, int length, Task window)
		{
			var completed = reader.Completion;
			length -= list.Count; // length left

			while (length > 0)
			{
				Drain(reader, ref list!, ref length);

				var ready = reader.WaitToReadAsync().AsTask();
				var evt = await Task.WhenAny(window, completed, ready);
				if (evt != ready) break;
			}
		}

		private static void Drain<T>(
			ChannelReader<T> reader, ref List<T>? list, ref int length)
		{
			while (length > 0 && reader.TryRead(out var item))
			{
				(list ??= new List<T>()).Add(item);
				length--;
			}
		}

		public static void Forget(this Task task)
		{
			task.ContinueWith(
				t => t.Exception, // clear exception so TPL stops complaining
				TaskContinuationOptions.NotOnRanToCompletion);
		}

		public static T Required<T>(this T argument, string argumentName) where T: class =>
			argument ?? throw new ArgumentNullException(argumentName);

		public static T[] EmptyIfNull<T>(this T[]? argument) =>
			argument ?? Array.Empty<T>();

		#if NETSTANDARD2_1_OR_GREATER || NET5_0_OR_GREATER
		[return: NotNullIfNotNull("fallback")]
		#endif
		public static TValue? TryGetOrDefault<TKey, TValue>(
			this IDictionary<TKey, TValue> dictionary, TKey key, TValue? fallback = default) =>
			dictionary.TryGetValue(key, out var result) ? result : fallback;

		public static void ForEach<T>(this IEnumerable<T> sequence, Action<T> action)
		{
			foreach (var item in sequence)
				action(item);
		}
	}
}
