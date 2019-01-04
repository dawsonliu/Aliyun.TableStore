using System.Collections.Generic;

namespace Aliyun.TableStore.DataModel.Search
{
    public class ColumnsToGet
    {
        public List<string> Columns { get; set; }
        public bool ReturnAll { get; set; }
    }
}
