using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Net;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;
using GraphView;
using Newtonsoft.Json.Linq;


namespace GroupQuery
{
    using linkTuple = Tuple<string, List<string>>;
    class GroupQueryTest
    {
        private linkTuple zeroTuple = new linkTuple("ALL", new List<string>());
        private DocumentClient client;
        private const string EndpointUrl = "https://graphview.documents.azure.com:443/";
        private const string PrimaryKey = "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==";
        static GroupQueryTest ins = new GroupQueryTest();

        static void Main(string[] args)
        {
            ins.init();
            //ins.showAll();
            ins.QueryTrianglePattern();
            //Console.ReadKey();
            //ins.QueryComplexPattern();
            Console.WriteLine("Ok!");
            Console.ReadKey();

        }
        #region basicOperator
        public void init()
        {
            this.client = new DocumentClient(new Uri(EndpointUrl), PrimaryKey);
        }
        public void showAll()
        {
            var all = ExcuteQuery("GraphMatch", "GraphTwo", "SELECT * FROM ALL");
            foreach (var x in all) Console.Write(x);
        }
        public IQueryable<dynamic> ExcuteQuery(string database, string collection, string script)
        {
            FeedOptions queryOptions = new FeedOptions { MaxItemCount = -1 };
            IQueryable<dynamic> Result = this.client.CreateDocumentQuery(
                    UriFactory.CreateDocumentCollectionUri(database, collection),
                    script,
                    queryOptions);
            return Result;
        }
        #endregion
        #region findLink
        public string at(string path, int pos)
        {
            return path.Substring(pos * 37 + 1, 36);
        }
        public linkTuple FindLink(linkTuple lastTuple, int bindto = -1, bool reverse = false)
        {
            bool initFlag = false;
            List<string> myPath = new List<string>();
            IQueryable<dynamic> res;
            string range = lastTuple.Item1;
            List<string> lastPath = lastTuple.Item2;
            if (lastPath.Count == 0) initFlag = true;
            string dest = "";
            string way = "";
            if (reverse) way = "reverse"; else way = "edge";
            string script = "SELECT {\"id\":node.id, \"edge\":node._edge, \"reverse\":node._reverse_edge} AS NodeInfo" +
                " FROM NODE node";

            if (range != "ALL") script += " WHERE node.id IN (" + range + ")";
            res = ins.ExcuteQuery("GraphMatch", "GraphTwo", script);

            foreach (var x in res)
            {
                JToken NodeInfo = JObject.Parse(JsonConvert.SerializeObject(x))["NodeInfo"];
                var edge = NodeInfo[way];
                var id = NodeInfo["id"];
                if (initFlag) lastPath.Add("/" + id.ToString());
                var idLength = id.ToString().Length;
                foreach (var y in edge)
                {
                    foreach (var s in lastPath) if (s.Substring(s.Length - idLength, idLength) == id.ToString())
                        {
                            if (bindto == -1)
                            {
                                if (s.IndexOf(y["_sink"].ToString()) == -1)
                                {
                                    dest = dest + ("\"" + y["_sink"]) + "\"" + ", ";
                                    myPath.Add(s + "/" + y["_sink"].ToString());
                                }
                            }
                            else
                            {
                                if (at(s, bindto) == y["_sink"].ToString())
                                {
                                    dest = dest + ("\"" + y["_sink"]) + "\"" + ", ";
                                    myPath.Add(s + "/" + y["_sink"].ToString());
                                }
                            }
                        }
                }
            }
            if (dest.Length != 0) return new Tuple<string, List<string>>(dest.Substring(0, dest.Length - 2), myPath);
            else return new Tuple<string, List<string>>(dest, myPath);
        }
        public linkTuple FindPrev(linkTuple link, int bindto = -1)
        {
            return FindLink(link, bindto, true);
        }
        public linkTuple FindPrevPath(linkTuple begin, int cnt)
        {
            linkTuple tempTuple = begin;
            for (int i = 0; i < cnt; i++)
            {
                tempTuple = FindPrev(tempTuple);
            }
            return tempTuple;
        }
        public linkTuple FindSucc(linkTuple link, int bindto = -1)
        {
            return FindLink(link, bindto, false);
        }
        public linkTuple FindSuccPath(linkTuple begin, int cnt)
        {
            linkTuple tempTuple = begin;
            for (int i = 0; i < cnt; i++)
            {
                tempTuple = FindSucc(tempTuple);
            }
            return tempTuple;
        }
        public linkTuple FindUnion(linkTuple link, int srcNode, int destNode = -1)
        {
            List<string> lastPath = link.Item2;
            List<string> myPath = new List<string>();
            List<string> srcSet = new List<string>();
            List<string> destSet = new List<string>();
            string src = "";
            string dest = "";
            foreach (string path in link.Item2)
                srcSet.Add(at(path, srcNode));
            if (destNode != -1)
                foreach (string path in link.Item2)
                    destSet.Add(at(path, destNode));
            foreach (string x in srcSet)
                src = src + "\"" + x + "\"" + ", ";
            foreach (string x in destSet)
                dest = dest + "\"" + x + "\"" + ", ";
            if (src.Length == 0) return link;
            src = src.Substring(0, src.Length - 2);
            string script = "SELECT {\"id\":node.id, \"edge\":node._edge, \"reverse\":node._reverse_edge} AS NodeInfo" +
                " FROM NODE node" +
                " WHERE node.id IN (" + src + ")";
            var res = ins.ExcuteQuery("GraphMatch", "GraphTwo", script);
            foreach (var x in res)
            {
                JToken NodeInfo = JObject.Parse(JsonConvert.SerializeObject(x))["NodeInfo"];
                var edge = NodeInfo["edge"];
                var id = NodeInfo["id"];
                bool avaliableFlag = false;
                if (destNode != -1)
                {
                    foreach (var y1 in edge)
                        if (dest.IndexOf(y1["_sink"].ToString()) != -1)
                        {
                            avaliableFlag = true;
                            foreach (var z in lastPath)
                            {
                                if (at(z, z.Length / 37 - 1) == id.ToString())
                                    myPath.Add(z + "/" + y1["_sink"].ToString());
                            }
                        }
                }
                else
                {
                    foreach (var y2 in edge)
                            foreach (var z in lastPath)
                                if (at(z, srcNode) == id.ToString() &&
                                            z.IndexOf(y2["_sink"].ToString()) == -1)
                                {
                                    myPath.Add(z + "/" + y2["_sink"].ToString());
                                    avaliableFlag = true;
                                }
                }
                    if (avaliableFlag == false)
                        link.Item2.RemoveAll((string str) => { return str.IndexOf(id.ToString()) == -1 ? false : true; });
            }
            return new linkTuple(link.Item1, myPath);
        }
        public HashSet<string> ExtractNodes(linkTuple link)
        {
            HashSet<string> res = new HashSet<string>();
            foreach (var path in link.Item2)
            {
                int cnt = path.Length / 37;
                for (int i = 0; i < cnt; i++)
                    res.Add(at(path, i));
            }
            return res;
        }
        #endregion
        #region patternMatch
        public void QueryTrianglePattern()
        {
            var q = ExtractNodes(FindSucc(FindSucc(FindSucc(zeroTuple)), 0));
            foreach (var x in q)
                Console.WriteLine(x);
        }
        public void QueryCubePattern()
        {
            var q = ExtractNodes(FindSucc(FindSuccPath(zeroTuple, 3), 0));
            foreach (var x in q)
                Console.WriteLine(x);
        }
        public void QueryComplexPattern()
        {
            var q = ExtractNodes(FindUnion(FindUnion(FindUnion(FindSucc(FindSucc(zeroTuple)),0,-1),0,-1),1,-1));
            foreach (var x in q)
                Console.WriteLine(x);
        }
        #endregion
    }
}
