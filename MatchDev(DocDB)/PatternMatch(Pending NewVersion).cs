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
    using BindingStatue = Dictionary<string, int>;
    using LinkStatue = Dictionary<string, List<string>>;
    using PathStatue = Tuple<Dictionary<string, int>, Dictionary<string, List<string>>>;
    using StageStatue = List<Tuple<Dictionary<string, int>, Dictionary<string, List<string>>>>;
    class GroupQueryComponent
    {
        private DocumentClient client;
        private const string EndpointUrl = "https://graphview.documents.azure.com:443/";
        private const string PrimaryKey = "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==";
        static GroupQueryComponent ins = new GroupQueryComponent();
        static List<string> ListZero = new List<string>();
        static LinkStatue LinkZero = new LinkStatue();
        static BindingStatue BindZero = new BindingStatue();
        static PathStatue PathZero = new Tuple<BindingStatue,LinkStatue>(BindZero,LinkZero);
        static StageStatue StageZero = new StageStatue() { PathZero };
        static void Main(string[] args)
        {
            LinkZero.Add("Bindings", new List<string>());
            ins.init();
            ins.QueryTrianglePattern();
            Console.WriteLine("Ok!");
            Console.ReadKey();

        }
        public void init()
        {
            this.client = new DocumentClient(new Uri(EndpointUrl), PrimaryKey);
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
        public void ShowAll()
        {
            var all = ExcuteQuery("GraphMatch", "GraphTwo", "SELECT * FROM ALL");
            foreach (var x in all) Console.Write(x);
        }
        public StageStatue FindLink(StageStatue LastStage, int from, int to)
        {
            StageStatue MiddleStage = new StageStatue();
            StageStatue CurrentStage = new StageStatue();
            LinkStatue QueryResult = new LinkStatue();

            string InRangeScript = "";
            foreach (var path in LastStage)
            {
                foreach (var BindingPair in path.Item1)
                    if (BindingPair.Value == from)
                    {
                        InRangeScript += "\"" + BindingPair.Key + "\"" + ",";
                    }
            }
            bool NotYetBind = InRangeScript.Length == 0;

            string script =
                "SELECT {\"id\":node.id, \"edge\":node._edge, \"reverse\":node._reverse_edge} AS NodeInfo" +
               " FROM NODE node";
            string WhereScript = NotYetBind ? "" : " WHERE node.id IN (" + InRangeScript.Substring(0, InRangeScript.Length - 1) + ")";
            script += WhereScript;
            var res = ins.ExcuteQuery("GraphMatch", "GraphTwo", script);

            foreach (var item in res)
            {
                JToken NodeInfo = ((JObject)item)["NodeInfo"];
                var edge = NodeInfo["edge"];
                var id = NodeInfo["id"];
                foreach (var y in edge)
                {
                    if (!QueryResult.ContainsKey(id.ToString()))
                        QueryResult.Add(id.ToString(), new List<string>());
                    QueryResult[id.ToString()].Add(y["_sink"].ToString()); 

                }
                if (NotYetBind) 
                    foreach(var path in LastStage)
                    {
                        if (!path.Item1.ContainsKey(id.ToString()))
                        {
                            BindingStatue newBinding = new BindingStatue(path.Item1);
                            LinkStatue newLink = new LinkStatue(path.Item2);
                            newLink["Bindings"].Add(from.ToString());
                            newBinding.Add(id.ToString(), from);
                            MiddleStage.Add(new PathStatue(newBinding, newLink));
                        }
                    }
            }

            if (!NotYetBind)
                MiddleStage = LastStage;

            foreach (var path in MiddleStage)
            {
                foreach (var BindingPair in path.Item1)
                    if (BindingPair.Value == from)
                    {
                        List<string> LinkOfStartNode = new List<string>();
                        if (QueryResult.TryGetValue(BindingPair.Key, out LinkOfStartNode))
                        {
                            foreach (var end in LinkOfStartNode)
                            {
                                int group = 0;

                                LinkStatue newLink = new LinkStatue(path.Item2);
                                if (!path.Item2.ContainsKey(BindingPair.Key))
                                    newLink.Add(BindingPair.Key, new List<string>());
                                newLink[BindingPair.Key].Add(end);
                                if (path.Item2["Bindings"].Contains(to.ToString()))
                                {
                                    if (path.Item1.TryGetValue(end, out group) && group == to)
                                        CurrentStage.Add(new PathStatue(path.Item1, newLink));
                                }
                                else
                                {
                                    newLink["Bindings"].Add(to.ToString());
                                    BindingStatue newBinding = new BindingStatue(path.Item1);
                                    newBinding.Add(end, to);
                                    CurrentStage.Add(new PathStatue(newBinding, newLink));
                                }
                            }
                        }
                    }
            }
            return CurrentStage;
        }
        public HashSet<string> ExtractNodes(StageStatue Stage)
        {
            HashSet<string> res = new HashSet<string>();
            foreach (var path in Stage)
            {
                foreach (var node in path.Item1)
                {
                    res.Add(node.Key);
                }
            }
            return res;
        }

        public void QueryTrianglePattern()
        {
            var q = ExtractNodes(FindLink(FindLink(FindLink(StageZero,1,2),2,3),3,1));
            foreach (var x in q)
                Console.WriteLine(x);
        }
    }
}

