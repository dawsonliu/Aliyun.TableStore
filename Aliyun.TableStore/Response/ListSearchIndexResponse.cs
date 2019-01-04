using System.Collections.Generic;
using Aliyun.TableStore.DataModel.Search;

namespace Aliyun.TableStore.Response
{
    /// <summary>
    /// 表示ListSearchIndex的返回
    /// </summary>
    public class ListSearchIndexResponse: OTSResponse
	{
		public ListSearchIndexResponse() { }

		public List<SearchIndexInfo> IndexInfos { get; set; }
	}
}
