using System.Threading.Tasks;

namespace SampleCalculation.BLL.Kafka
{
    public interface IKafkaSender
    {
        Task SendAsync(string topic, object message);
    }
}
