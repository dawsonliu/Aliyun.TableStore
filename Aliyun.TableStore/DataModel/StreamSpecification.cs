using System;
namespace Aliyun.TableStore.DataModel
{
    public class StreamSpecification
    {
        public bool EnableStream { get; set; }

        public int ExpirationTime { get; set; }

        public StreamSpecification( bool enableStream)
        {
            EnableStream = enableStream;
        }
    }
}
