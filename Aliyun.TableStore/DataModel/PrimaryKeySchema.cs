﻿/*
 *  Trade secret of Alibaba Group R&D.
 *  Copyright (c) 2015 Alibaba Group R&D. 
 *
 *  All rights reserved.  This notice is intended as a precaution against
 *  inadvertent publication and does not imply publication or any waiver
 *  of confidentiality.  The year included in the foregoing notice is the
 *  year of creation of the work.
 *
 */

using System;
using System.Collections.Generic;

namespace Aliyun.TableStore.DataModel
{
    /// <summary>
    /// 表示主键的设计，包括每个主键列的名称和列值类型。
    /// </summary>
    public class PrimaryKeySchema : List<Tuple<string, ColumnValueType>> 
    {
        /// <summary>
        /// 添加一个主键列的设计。
        /// </summary>
        /// <param name="primaryKeyColumnName">列名</param>
        /// <param name="columnValueType">列值类型</param>
        public void Add(string primaryKeyColumnName, ColumnValueType columnValueType)
        {
            var tuple = new Tuple<string, ColumnValueType>(primaryKeyColumnName, columnValueType);
            Add(tuple);
        }
    }
}
