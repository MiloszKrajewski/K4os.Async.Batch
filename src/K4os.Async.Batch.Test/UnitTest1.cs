using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace K4os.Async.Batch.Test
{
	public class UnitTest1
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
	}
}
