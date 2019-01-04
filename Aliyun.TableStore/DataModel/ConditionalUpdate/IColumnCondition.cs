using Google.ProtocolBuffers;
using Aliyun.TableStore.DataModel.Filter;

namespace Aliyun.TableStore.DataModel.ConditionalUpdate
{
    public interface IColumnCondition
    {
        ColumnConditionType GetConditionType();
        ByteString Serialize();
        IFilter ToFilter();
    }
}
