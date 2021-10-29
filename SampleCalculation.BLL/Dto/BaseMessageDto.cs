using SampleCalculation.BLL.Enum;
using System;

namespace SampleCalculation.BLL.Dto
{
    public class BaseMessageDto
    {
        public Guid TrxId { get; set; }
        public DateTime Timestamp { get; set; }
        public string Activity { get; set; }
        public EnumStatus Status { get; set; }
        public string Message { get; set; }
    }
}
