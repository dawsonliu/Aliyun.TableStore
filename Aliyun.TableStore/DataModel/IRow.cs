using System;
namespace Aliyun.TableStore.DataModel
{
    public interface IRow : IComparable<IRow>
    {
        PrimaryKey GetPrimaryKey();
    }
}
