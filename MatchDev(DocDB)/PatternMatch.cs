﻿using System;
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

using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;

namespace GroupQuery
{
    using BindingStatue = Dictionary<string, int>;
    using LinkStatue = Dictionary<string, HashSet<string>>;
    using PathStatue = Tuple<Dictionary<string, int>, Dictionary<string, HashSet<string>>>;

    class GroupQueryComponent
    {
        static private DocumentClient client;
        private const string EndpointUrl = "https://graphview.documents.azure.com:443/";
        private const string PrimaryKey = "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==";
        static GroupQueryComponent ins = new GroupQueryComponent();
        static List<string> ListZero = new List<string>();
        static LinkStatue LinkZero = new LinkStatue();
        static BindingStatue BindZero = new BindingStatue();
        static PathStatue PathZero = new Tuple<BindingStatue, LinkStatue>(BindZero, LinkZero);
        static List<PathStatue> StageZero = new List<PathStatue>() { PathZero };

        static void Main(string[] args)
        {
            LinkZero.Add("Bindings", new HashSet<string>());
            ins.init();
            ins.QueryTrianglePattern();
            Console.WriteLine("Ok!");
            Console.ReadKey();

        }
        public void init()
        {
            client = new DocumentClient(new Uri(EndpointUrl), PrimaryKey);
        }
        public IQueryable<dynamic> ExcuteQuery(string database, string collection, string script)
        {
            FeedOptions queryOptions = new FeedOptions { MaxItemCount = -1 };
            IQueryable<dynamic> Result = client.CreateDocumentQuery(
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
        public IEnumerable<PathStatue> FindLink(IEnumerable<PathStatue> LastStage, int from, int to, HashSet<int> ReverseCheckSet = null)//,string From, string where)
        {
            LinkStatue QueryResult = new LinkStatue();
            List<PathStatue> MiddleStage = new List<PathStatue>();
            // For start nodes which has been binded
            string InRangeScript = "";
            foreach (var path in LastStage)
            {
                MiddleStage.Add(path);
                foreach (var BindingPair in path.Item1)
                    if (BindingPair.Value == from)
                    {
                        InRangeScript += "\"" + BindingPair.Key + "\"" + ",";
                    }
            }

            bool NotYetBind = InRangeScript.Length == 0;
            // To find not yet binded start nodes and possible end nodes
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
                var reverse = NodeInfo["reverse"];
                bool ReverseVailed = true;
                if (ReverseCheckSet != null)
                {
                    foreach (var path in LastStage)
                    {
                        foreach (var y in reverse)
                        {
                            if (path.Item1.ContainsKey(y["_sink"].ToString()))
                            {
                                if (!ReverseCheckSet.Contains(path.Item1[y["_sink"].ToString()]))
                                    ReverseVailed = false;
                            }
                        }
                    }
                }
                if (ReverseVailed)
                {
                    // Construct adj list for current statue
                    foreach (var y in edge)
                    {
                        if (!QueryResult.ContainsKey(id.ToString()))
                            QueryResult.Add(id.ToString(), new HashSet<string>());
                        QueryResult[id.ToString()].Add(y["_sink"].ToString());

                    }
                    // If no start nodes has been binded to the giving group, bind each unbinded node to start node
                    if (NotYetBind)
                        foreach (var path in LastStage)
                        {
                            if (!path.Item1.ContainsKey(id.ToString()))
                            {
                                BindingStatue newBinding = new BindingStatue(path.Item1);
                                newBinding.Copy();
                                newBinding.Add(id.ToString(), from);
                                LinkStatue newLink = new LinkStatue();
                                HashSet<string> newList;
                                foreach (var x in path.Item2)
                                {
                                    newList = new HashSet<string>(x.Value);
                                    newLink.Add(x.Key, newList);
                                }
                                newLink["Bindings"].Add(from.ToString());
                                PathStatue newPath = new PathStatue(newBinding, newLink);
                                MiddleStage.Add(newPath);
                            }
                        }
                }
                // For each path in current stage
                foreach (var path in MiddleStage)
                {

                    // For each binded start node
                    foreach (var BindingPair in path.Item1)
                        if (BindingPair.Value == from)
                        {
                            HashSet<string> LinkOfStartNode = new HashSet<string>();
                            if (QueryResult.TryGetValue(BindingPair.Key, out LinkOfStartNode))
                            {
                                // For each node link to start nodes
                                foreach (var end in LinkOfStartNode)
                                {
                                    int group = 0;

                                    LinkStatue newLink = new LinkStatue();
                                    HashSet<string> newList;
                                    foreach (var x in path.Item2)
                                    {
                                        newList = new HashSet<string>(x.Value);
                                        newLink.Add(x.Key, newList);
                                    }
                                    // If end group has been binded to some nodes
                                    if (path.Item2["Bindings"].Contains(to.ToString()))
                                    {
                                        // Determine if the node is in the end group and construct new path
                                        if (path.Item1.TryGetValue(end, out group) && group == to)
                                        {
                                            // Construct new Link
                                            if (!path.Item2.ContainsKey(BindingPair.Key))
                                                newLink.Add(BindingPair.Key, new HashSet<string>());
                                            newLink[BindingPair.Key].Add(end);
                                            yield return new PathStatue(path.Item1, newLink);
                                        }
                                    }
                                    else
                                    {
                                        // if no node was binded to end group
                                        newLink["Bindings"].Add(to.ToString());
                                        // Construct new Link
                                        if (!path.Item2.ContainsKey(BindingPair.Key))
                                            newLink.Add(BindingPair.Key, new HashSet<string>());
                                        newLink[BindingPair.Key].Add(end);
                                        // Bind the selected node to end group
                                        BindingStatue newBinding = new BindingStatue(path.Item1);
                                        newBinding.Add(end, to);
                                        yield return new PathStatue(newBinding, newLink);
                                    }
                                }
                            }
                        }
                }
            }
            yield break;
        }
        public HashSet<string> ExtractNodes(IEnumerable<PathStatue> Stage)
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
            var q = ExtractNodes(FindLink(FindLink(FindLink(StageZero, 1, 2), 2, 3), 3, 1));
            foreach (var x in q)
                Console.WriteLine(x);
        }
    }
}

