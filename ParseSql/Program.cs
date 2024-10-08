using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
//using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
//using System.Threading.Tasks;
using System.Xml;
//using System.Xml.Linq;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace ParseSql
{
    public static class MyExtensions
    {
        public static string QuoteStrting(this string str)
        {
            return "[" + str + "]";
        }
        public static string TrnSpecChars(this string str)
        {
            return str.Replace("<", "&lt;").Replace(">", "&gt;").Replace("&", "&#38;").Replace("'", "&#39;").Replace("\"", "&#34;");
        }

    }
    class Program
    {
        static StringBuilder result = new StringBuilder();
        static LogWriter logwr = new LogWriter("");

        static void Main(string[] args)
        {

            List<string> dbList = new List<string> { "DW_DMA" , "DW_PREP_DMA", "MYDW_LOG" };
            //{ "DW_DMA", "DW_DW", "DW_PREP_DMA", "FDB", "FT_SALG", "FT_SALG", "FT_SALG_DMA", "MYDW_HIST", "MYDW_PREP" };

            string ReadDbServer = "MYSQLSERVER\\DEV";
            string sComponent;
            string sqlText;
            string sqlid;
            bool ExecIsQuotedIdentOn;

            string sqlSProcQuery =
                @"SELECT m.object_id
                    , null sqlid
	                , QUOTENAME(OBJECT_SCHEMA_NAME(m.object_id)) as [schema]
	                , QUOTENAME(OBJECT_NAME(m.object_id)) AS ObjectName
	                , cast(OBJECTPROPERTY(m.object_id, 'ExecIsQuotedIdentOn') as bit) AS ExecIsQuotedIdentOn
	                , definition
                FROM sys.sql_modules m
				join sys.objects o on o.object_id = m.object_id 
                where o.type not in ('V')
                --and m.object_id = object_id('[dbo].[TableXY]')
                ;";

            string sqlMetaTabQuery =
                @"SELECT 
                    id sqlid,
                    '' [schema],
                    Package+'\'+ objectname ObjectName,
                    cast(1 as bit) as ExecIsQuotedIdentOn,
                    replace(content,'?','null') definition 
                    FROM [MYDW_META].[dbo].[ssisPkgMeta2]
                    where 1=1
	                and	((contenttype in ('SqlStatementSource','SqlCommand','SqlCommandParam') and content not like '%::%')
	                or (contenttype = 'VariableValue' and content like '%SELECT%' or content like '%INSERT%' or content like '%UPDATE%' or content like '%DELETE%' ))                    
                    --and id = 786
                    ";

            DataTable sqlObjectsTable;

            foreach (string ReadDb in dbList)
            {
                Console.WriteLine("Current Database:" + ReadDb);
                    
                sqlObjectsTable = getDataTableFromSQLs(ReadDbServer, ReadDb, sqlSProcQuery);

                foreach (DataRow row in sqlObjectsTable.Rows)
                {
                    sComponent = ((row["schema"].ToString() != "") ? row["schema"].ToString() + "." : "") + row["ObjectName"].ToString();
                    sqlText = row["definition"].ToString();
                    sqlid = row["sqlid"].ToString();
                    ExecIsQuotedIdentOn = bool.Parse(row["ExecIsQuotedIdentOn"].ToString());

                    Console.WriteLine("Parsing component:" + sComponent);

                    XmlDocument doc = ParseSQL(sqlid, sqlText, ExecIsQuotedIdentOn);

                    ExtractTabledependencies(doc, ReadDbServer, ReadDb, sqlid, sComponent);

                }
            }

            string ReadDb2 = "MYDW_META";
            sqlObjectsTable = getDataTableFromSQLs(ReadDbServer, ReadDb2, sqlMetaTabQuery);

            //DataTable sqlObjectsTable = getDataTableFromSQLTable(ReadDbServer, ReadDb);


            foreach (DataRow row in sqlObjectsTable.Rows)
                {
                    sComponent = ((row["schema"].ToString() != "")?row["schema"].ToString() + ".":"") + row["ObjectName"].ToString();
                    sqlText = row["definition"].ToString();
                    sqlid = row["sqlid"].ToString();
                    ExecIsQuotedIdentOn = bool.Parse(row["ExecIsQuotedIdentOn"].ToString());

                    Console.WriteLine("Parsing component:" + sComponent);

                    XmlDocument doc = ParseSQL(sqlid, sqlText, ExecIsQuotedIdentOn);

                    ExtractTabledependencies(doc, ReadDbServer, ReadDb2, sqlid, sComponent);

                }
            
        }



        static DataTable getDataTableFromSQLs(string ReadDbServer, string ReadDb, string sqlSelectObjectsQuery)
        {
            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder
            {
                DataSource = ReadDbServer,
                InitialCatalog = ReadDb,
                IntegratedSecurity = true
            };

            DataTable sqlObjectsTable = new DataTable();
            SqlConnection conn = new SqlConnection(builder.ToString());
            conn.Open();
            SqlCommand sqlCommand = new SqlCommand(sqlSelectObjectsQuery, conn);

            SqlDataAdapter da = new SqlDataAdapter(sqlCommand);

            da.Fill(sqlObjectsTable);
            conn.Close();
            da.Dispose();

            return sqlObjectsTable;
        }


        static XmlDocument ParseSQL (string sqlid, string sql, bool ExecIsQuotedIdentOn)
        {
            try
            {
                TSql150Parser parser = new TSql150Parser(ExecIsQuotedIdentOn, SqlEngineType.All);
                TSqlFragment tree = parser.Parse(new System.IO.StringReader(sql), out IList<ParseError> errors);

                foreach (ParseError err in errors)
                {
                    Console.WriteLine("Parse error in {0}: {1}", sqlid, err.Message);
                    logwr.LogWrite("Parse error in {0}: {1}", sqlid, err.Message);
                }

                if (tree.ScriptTokenStream != null) {

                    result = new StringBuilder();

                    ScriptDomWalk(tree, "root");

                    XmlDocument doc = new XmlDocument();
                    doc.LoadXml(result.ToString());

                    return doc;

                }
                else
                {
                    return null;
                } 
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception in sql2xml. Message: {0}", ex.Message);
                logwr.LogWrite("Exception in sql2xml. Message: {0}", ex.Message);
                return null;
            }
        }

        static void ExtractTabledependencies (XmlDocument doc, string ReadDbServer, string ReadDb, string sqlid, string sqlSrcRef)
        {
            string ndPath, idPath;

            XmlNode xroot = doc.DocumentElement;

            /** TRUNCATE  **/ //normal selects
            ndPath = "//TruncateTableStatement";
            idPath = "SchemaObjectName//Identifier [@memberName!='Identifiers']";
            InsertNTRs(sqlid, "Table", "TRUNCATE", ReadDbServer, ReadDb, sqlSrcRef, ndPath, idPath, xroot);

            /** SELECTS  **/ //normal selects
            ndPath = "//SelectStatement//FromClause//NamedTableReference";
            idPath = "SchemaObjectName//Identifier [@memberName!='Identifiers']";
            InsertNTRs(sqlid, "Table", "SELECT", ReadDbServer, ReadDb, sqlSrcRef, ndPath, idPath, xroot);

            // select into variation 1
            ndPath = "//SelectStatement//FromClause//QueryDerivedTable//FromClause//NamedTableReference";
            idPath = "SchemaObjectName//Identifier [@memberName!='Identifiers']";
            InsertNTRs(sqlid, "Table", "SELECT", ReadDbServer, ReadDb, sqlSrcRef, ndPath, idPath, xroot);

            /** INSERT **/ // insert select
            ndPath = "//InsertStatement//SelectInsertSource//FromClause//NamedTableReference";
            idPath = "SchemaObjectName//Identifier [@memberName!='Identifiers']";
            InsertNTRs(sqlid, "Table", "SELECT", ReadDbServer, ReadDb, sqlSrcRef, ndPath, idPath, xroot);

            // select into 
            ndPath = "//SelectStatement";
            idPath = "SchemaObjectName [@memberName='Into']//Identifier [@memberName!='Identifiers']";
            InsertNTRs(sqlid, "Table", "INSERT", ReadDbServer, ReadDb, sqlSrcRef, ndPath, idPath, xroot);

            ndPath = "//InsertStatement//NamedTableReference [@memberName = 'Target']";
            idPath = "SchemaObjectName//Identifier [@memberName!='Identifiers']";
            InsertNTRs(sqlid, "Table", "INSERT", ReadDbServer, ReadDb, sqlSrcRef, ndPath, idPath, xroot);

            /** UPDATES **/
            ndPath = "//UpdateStatement//NamedTableReference";
            idPath = "SchemaObjectName//Identifier [@memberName!='Identifiers']";
            InsertNTRs(sqlid, "Table", "UPDATE", ReadDbServer, ReadDb, sqlSrcRef, ndPath, idPath, xroot);

            /** DELETES **/
            ndPath = "//DeleteStatement//NamedTableReference";
            idPath = "SchemaObjectName//Identifier [@memberName!='Identifiers']";
            InsertNTRs(sqlid, "Table", "DELETE", ReadDbServer, ReadDb, sqlSrcRef, ndPath, idPath, xroot);

            /** EXECs **/
            ndPath = "//ExecuteStatement//ExecutableProcedureReference";
            idPath = "ProcedureReferenceName//Identifier [@memberName!='Identifiers']";
            InsertNTRs(sqlid, "StoredProcedure", "EXEC", ReadDbServer, ReadDb, sqlSrcRef, ndPath, idPath, xroot);

        }


        static void InsertNTRs(string sqlid, string refObjType, string operType, string dbInst, string dbName, string sqlComp, string xmlNodes, string xmlIdentPath, XmlNode xnode)
        {
            SqlConnectionStringBuilder sInserter = new SqlConnectionStringBuilder
            {
                DataSource = "MYSQLSERVER\\DEV",
                InitialCatalog = "MYDW_META",
                IntegratedSecurity = true
            };

            string InsertSql =
                "INSERT INTO [SQLOBJECTDEPENDENCIES] " +
                "([SQLID],[DBINSTANCE],[DBNAME],[SQLOBJECT]," +
                "[OPERATION],[NODES_SELECTION],[IDENTS],[REFINSTANCE],[REFDB],[REFSCHEMA],[REFOBJECTNAME],[REFOBJECTTYPE]) " +
                "VALUES (@sqlid, @dbinstance,@dbname,@component," +
                "@oper,@nodes,@idents,@server,@database,@schema,@object,@objtype)";

            string servername;
            string database;
            string schema;
            string objectname;
            string atValue;

            try
            {

                SqlConnection insert_conn = new SqlConnection(sInserter.ToString());
                insert_conn.Open();
                SqlCommand insert_cmd = new SqlCommand(InsertSql, insert_conn);

                var ntrs = xnode.SelectNodes(xmlNodes);
                foreach (XmlNode n in ntrs)
                {
                    var identifiers = n.SelectNodes(xmlIdentPath);

                    servername = null;
                    database = null;
                    schema = null;
                    objectname = null;

                    foreach (XmlNode idNode in identifiers)
                    {
                        var at = idNode.SelectSingleNode("text()");
                        if (at != null)
                        {
                            atValue = at.Value.Trim().QuoteStrting();
                            foreach (XmlAttribute att in idNode.Attributes)
                            {
                                switch (att.Value.ToString())
                                {
                                    case "ServerIdentifier": servername = atValue; break;
                                    case "DatabaseIdentifier": database = atValue; break;
                                    case "SchemaIdentifier": schema = atValue; break;
                                    case "BaseIdentifier": objectname = atValue; break;
                                }
                            }
                        }
                    }
                    if (objectname != null)
                    {
                        //Console.WriteLine("\"{0}\",\"{1}\",\"{2}.{3}.{4}\",\"{5}\"", operType, servername, database, schema, table, sqlComp);
                        insert_cmd.Parameters.Add("@sqlid", SqlDbType.NVarChar).Value = sqlid;
                        insert_cmd.Parameters.Add("@dbinstance", SqlDbType.NVarChar).Value = dbInst;
                        insert_cmd.Parameters.Add("@dbname", SqlDbType.NVarChar).Value = dbName;
                        insert_cmd.Parameters.Add("@oper", SqlDbType.NVarChar).Value = operType;
                        insert_cmd.Parameters.Add("@server", SqlDbType.NVarChar).Value = ((servername == null) ? (object)DBNull.Value : servername);
                        insert_cmd.Parameters.Add("@database", SqlDbType.NVarChar).Value = ((database == null) ? (object)DBNull.Value : database); 
                        insert_cmd.Parameters.Add("@schema", SqlDbType.NVarChar).Value = ((schema == null) ? (object)DBNull.Value : schema);
                        insert_cmd.Parameters.Add("@object", SqlDbType.NVarChar).Value = objectname;
                        insert_cmd.Parameters.Add("@objtype", SqlDbType.NVarChar).Value = refObjType;
                        insert_cmd.Parameters.Add("@nodes", SqlDbType.NVarChar).Value = xmlNodes;
                        insert_cmd.Parameters.Add("@idents", SqlDbType.NVarChar).Value = xmlIdentPath;
                        insert_cmd.Parameters.Add("@component", SqlDbType.NVarChar).Value = sqlComp;
                        insert_cmd.ExecuteNonQuery();
                        insert_cmd.Parameters.Clear();
                    }
                }

                insert_conn.Close();
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception in comp {0}\nMessage: {1}", sqlComp, ex.Message);
                logwr.LogWrite("Exception in comp {0}\nMessage: {1}", sqlComp, ex.Message);
            }
        }

        private static void ScriptDomWalk(object fragment, string memberName)
        {
            if (fragment.GetType().BaseType.Name != "Enum")
            {
                result.AppendLine("<" + fragment.GetType().Name + " memberName = '" + memberName + "'>");
            }
            else
            {
                result.AppendLine("<" + fragment.GetType().Name + "." + fragment.ToString().TrnSpecChars() + "/>");
                return;
            }

            Type t = fragment.GetType();

            PropertyInfo[] pibase;
            if (null == t.BaseType)
            {
                pibase = null;
            }
            else
            {
                pibase = t.BaseType.GetProperties();
            }

            foreach (PropertyInfo pi in t.GetProperties())
            {
                if (pi.GetIndexParameters().Length != 0)
                {
                    continue;
                }

                if (pi.PropertyType.BaseType != null)
                {
                    if (pi.PropertyType.BaseType.Name == "ValueType")
                    {
                        if(pi.GetValue(fragment, null) != null)
                        { 
                            result.Append("<" + pi.Name + ">" + pi.GetValue(fragment, null).ToString().TrnSpecChars() + "</" + pi.Name + ">");
                        }
                        continue;
                    }
                }

                if (pi.PropertyType.Name.Contains(@"IList`1"))
                {
                    if ("ScriptTokenStream" != pi.Name)
                    {
                        var listMembers = pi.GetValue(fragment, null) as IEnumerable<object>;

                        foreach (object listItem in listMembers)
                        {
                            ScriptDomWalk(listItem, pi.Name);
                        }
                    }
                }
                else
                {
                    object childObj = pi.GetValue(fragment, null);

                    if (childObj != null)
                    {
                        if (childObj.GetType() == typeof(string))
                        {
                            result.Append(pi.GetValue(fragment, null).ToString().TrnSpecChars());
                        }
                        else
                        {
                            ScriptDomWalk(childObj, pi.Name);
                        }
                    }
                }
            }

            result.AppendLine("</" + fragment.GetType().Name + ">");
        }
    }

    internal class SQLVisitor : TSqlFragmentVisitor
    {
        public override void ExplicitVisit(SelectStatement node)
        {
            var q = (node.QueryExpression) as QuerySpecification;
            var t = q.FromClause.TableReferences;
            var t1 = (t.FirstOrDefault()) as NamedTableReference ;
            //Console.WriteLine("select:" + t1.SchemaObject.DatabaseIdentifier.Value);
            //Console.WriteLine("select:" + t1.SchemaObject.SchemaIdentifier.Value);
            Console.WriteLine("select:" + t1.SchemaObject.BaseIdentifier.Value); 

        }

        public override void ExplicitVisit(UpdateStatement node)
        {
            //node.UpdateSpecification 
            Console.WriteLine("Update:" + node.ToString());
        }

        public override void ExplicitVisit(InsertStatement node)
        {

            var t = node.InsertSpecification.Target;
            Console.WriteLine(t.ToString());


            var source = node.InsertSpecification.InsertSource as ValuesInsertSource;
            if (source != null && source.IsDefaultValues)
                return;
        }

        public override void ExplicitVisit(DeleteStatement node)
        {
            Console.WriteLine("delete:" + node.ToString());
        }

        public override void Visit(TSqlFragment node)
        {
            Console.WriteLine("v:" + node.ToString());
        }

    }

}
