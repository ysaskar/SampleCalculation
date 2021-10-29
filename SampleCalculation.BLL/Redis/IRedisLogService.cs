using SampleCalculation.BLL.Dto;
using SampleCalculation.BLL.Enum;
using System;
using System.Threading.Tasks;

namespace SampleCalculation.BLL.Redis
{
    public interface IRedisLogService
    {
        Task LogBegin(BaseMessageDto dto, string logKey, DateTime begin);

        Task LogError(BaseMessageDto dto, string logKey, Exception ex);

        Task LogFailed(BaseMessageDto dto, string logKey, string msg);

        Task LogFinish(BaseMessageDto dto, string logKey);

        Task LogToRedis(BaseMessageDto dto, string logKey, EnumStatus status, DateTime? begin = null, string msg = null);
    }
}
