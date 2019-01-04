using System;
using Google.ProtocolBuffers;

namespace Aliyun.TableStore.DataModel.Filter
{
    public interface IFilter
    {
        FilterType GetFilterType();

        ByteString Serialize();
    }
}
