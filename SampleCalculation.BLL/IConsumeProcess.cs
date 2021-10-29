using Confluent.Kafka;
using System.Threading;
using System.Threading.Tasks;

namespace SampleCalculation.BLL
{
    public interface IConsumeProcess
    {
        Task ConsumeAsync<TKey>(ConsumeResult<TKey, string> consumeResult, CancellationToken cancellationToken = default);
    }
}
