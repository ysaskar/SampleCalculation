using System;

namespace SampleCalculation.BLL.Dto
{
    public class ValidationDto
    {
        public string ContractId { get; set; }
        public DateTime Timestamp { get; set; }
        public BaseMessageDto Metadata { get; set; }
    }
}
