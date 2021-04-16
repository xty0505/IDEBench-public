import random
from collections import OrderedDict

import numpy as np
import pprint
import json

from common.operation import Operation
from workflowgen.vizaction import VizAction
from workflowgen.linkaction import LinkAction
from workflowgen.selectionaction import SelectionAction
from optparse import OptionParser
import pandas as pd
from common.schema import Schema
from common.vizgraph import VizGraph
# from common.storage import Storage
import pandasql


class WorkflowGenerator:

    def __init__(self):

        parser = OptionParser()
        parser.add_option("-r", "--seed", dest="seed", action="store", type=int, help="Random seed", default=15000)
        parser.add_option("-d", "--dataset", dest="data_folder", action="store", help="path to save the file", default="flights")
        parser.add_option("--debug", dest="debug", action="store_true", help="creates a debug file", default=False)
        parser.add_option("-n", "--num-operations", dest="num_operations", action="store", type=int, help="Number of operations to generate", default=20)
        parser.add_option("-c", "--workflow-type", dest="config", action="store", help="path to config file", default="data/flights/workflowtypes/sequential.json")
        parser.add_option("-p", "--output", dest="path", action="store", help="path to save the file", default="workflow.json")
        parser.add_option("-s", "--num-samples", dest="numsamples", action="store", type=int, help="Number of samples to draw from the original dataset", default=10000)

        # crossfilter
        parser.add_option("--qn", dest="num_queries", action='store', type=int, help="Number of queries to generate", default=100)
        parser.add_option("--cf", dest="cf", action='store_true', help='Whether to generate crossfilter workload', default=False)
        parser.add_option("--session", dest="session", action="store", type=int, help="Number of query in a session", default=5)
        parser.add_option("--viz-n", dest="viz_number", action="store", type=int, help="Number of visualization", default=4)
        parser.add_option("--upbound", dest="upbound", action="store", type=int, help="Number of maximum upbound", default=4)

        (options, args) = parser.parse_args()
        self.options = options

        random.seed(options.seed)
        np.random.seed(seed=options.seed)

        print("data/" + options.data_folder + "/" + options.config)
        with open("data/" + options.data_folder + "/workflowtypes/" + options.config, "r") as fp:
            self.config = json.load(fp)

        schema = None
        with open(self.get_schema_path()) as f:
            schema = Schema(json.load(f))

        print("reading csv...")
        # load sample data
        df = pd.read_csv("data/" + options.data_folder + "/sample1.csv", nrows=options.numsamples, header=0)
        columns = []
        for c in df.columns:
            column = '_'.join(c.split(' '))
            columns.append(column)
        df.columns = columns
        
        #schema = {"tables": [{ "name": "df", "dimensions": []}]}
        sample_json = None
        with open("data/" + options.data_folder + "/sample.json", "r") as f:
            sample_json = json.load(f)
    #        for field in sample_json["tables"]["fact"]["fields"]:
    #          schema["tables"][0]["dimensions"].append({"name": field["field"]})


        #storage = Storage(schema)

        if self.options.cf:
            states = self.generate_cf_workloads(df, schema, sample_json)
        else:
            zero_qs_ratio = 100

            tries = -1
            while zero_qs_ratio > 0.8:
                tries += 1
                num_zeros_qs = 0
                num_qs = 0
                VizAction.VIZ_COUNTER = -1
                LinkAction.FIRST_LINK = None
                LinkAction.LATEST_LINK = None
                LinkAction.LINKS = set()

                vizgraph = VizGraph()
                random.seed(options.seed + tries)
                root = VizAction(self.options, self.config, df, vizgraph, schema, sample_json)
                current = root
                states = []

                num_ops = 0

                debug_states = []
                while num_ops < options.num_operations:
                    print(num_ops)
                    res = current.get_states()
                    if res:
                        affected_vizs = vizgraph.apply_interaction(res)
                        if options.debug:
                            nodes_dict = vizgraph.get_nodes_dict()
                            states_dict = {}
                            for n in nodes_dict.keys():
                                states_dict[n] = {
                                    "name":n,
                                    "source" : nodes_dict[n].get_source(),
                                    "binning":  nodes_dict[n].binning,
                                    "agg": nodes_dict[n].per_bin_aggregates,
                                    "selection": nodes_dict[n].get_selection(),
                                    "filter": nodes_dict[n].get_filter(),
                                    "computed_filter": nodes_dict[n].get_computed_filter_as_sql(schema),
                                }
                            debug_states.append(states_dict)

                        for x in affected_vizs:
                            sql = x.get_computed_filter_as_sql(schema).replace("FLOOR", "ROUND").replace(schema.get_fact_table_name(), "df")
                            r = pandasql.sqldf(sql, locals())
                            num_qs += 1
                            if len(r.index) == 0:
                                num_zeros_qs += 1

                        states.append(res.data)
                        #if "source" not in res:
                        num_ops += 1

                    current = current.get_next()
                    if current is None:
                        zero_qs_ratio = num_zeros_qs/num_qs
                        break
                zero_qs_ratio = num_zeros_qs/num_qs
                print(zero_qs_ratio)

        with open("data/" + options.data_folder +  "/workflows/" + options.path + ".json", "w") as fp:
            # fp.write(json.dumps({"name": "generated", "dataset": options.data_folder, "seed": options.seed, "config": options.config, "interactions": states}))
            json.dump({"name": "generated", "dataset": options.data_folder, "seed": options.seed, "config": options.config, "interactions": states}, fp, indent=4)

        print("done.")
        #with open("workflowviewer/public/workflow.json", "w") as fp:
        #    fp.write(json.dumps({"name": "generated", "dataset": options.data_folder, "seed": options.seed, "config": options.config, "interactions": states}))

        #with open("workflowviewer/public/workflow_debug.json", "w") as fp:
        #    fp.write(json.dumps(debug_states))

        #if options.debug:
        #    import webbrowser
        #    url = "http://localhost:3000"
        #    webbrowser.open(url)

    def get_schema_path(self):
        return "data/%s/sample.json" % (self.options.data_folder)

    def get_viz_name(self):
        return "viz_%i" % self.config["viz_counter"]

    # crossfilter
    def generate_cf_workloads(self, df, schema, sample_json):
        tries = -1
        num_ops = 0
        num_query = 0
        states = []
        sqls = []
        vizgraph = VizGraph()
        self.init_cf(vizgraph, states, df, schema, sample_json)

        current = SelectionAction(self.options, self.config, df, vizgraph, schema, sample_json)
        while num_ops < self.options.num_operations and num_query < self.options.num_queries:
            res = current.get_states()
            if res:
                affected_vizs = vizgraph.apply_interaction(res)
                for viz in affected_vizs:
                    tries += 1
                    random.seed(self.options.seed + tries)
                    np.random.seed(self.options.seed + tries)
                    operation = Operation(OrderedDict({"name": ("%s" % viz.name), "filter": viz.computed_filter}))
                    states.append(operation.data)
                    sqls.append(viz.get_computed_filter_as_sql(schema))
                    num_query += 1
                    print(num_query)
            num_ops += 1
            current = current.get_next()
        with open("data/" + self.options.data_folder + "/workflows/" + self.options.path + ".txt", "w") as f:
            for sql in sqls:
                f.write(sql)
                f.write('\n')
            f.close()

        return states

    def init_cf(self, vizgraph, states, df, schema, sample_json):
        # initialize visualizations
        dim_to_type = {}
        for field in sample_json["tables"]["fact"]["fields"]:
            dim_to_type[field["field"]] = field["type"]
        dims = list(self.config['dimensions'])
        selected_dims = np.random.choice(dims, size=self.options.viz_number, replace=False)
        for dim in selected_dims:
            d_bin = {'dimension': dim['name']}
            if dim_to_type[dim["name"]] == "quantitative":
                dim_max_val = df[dim["name"]].max()
                dim_min_val = df[dim["name"]].min()
                #d_bin["width"] = round(random.uniform(0.025, 0.1) * (dim_max_val - dim_min_val))
                d_bin["width"] = round(random.uniform(0.025, 0.1) * (dim_max_val - dim_min_val))
            elif dim_to_type[dim["name"]] == "categorical":
                try:
                    pd.to_numeric(df[dim["name"]])
                    d_bin["width"] = 1
                except:
                    pass
            elif dim_to_type[dim["name"]] == "temporal":
                pass

            def pick(choices, pd=None):
                if pd is None:
                    return random.choice(choices)

                total = sum(pd)
                r = random.uniform(0, total)
                upto = 0
                for i, c in enumerate(choices):
                    if upto + pd[i] >= r:
                        return c
                    upto += pd[i]
                assert False, "Shouldn't get here"

            per_bin_aggregate_type = pick(self.config["perBinAggregates"]["values"], self.config["perBinAggregates"]["pd"])
            per_bin_aggregate = {"type": per_bin_aggregate_type}
            if per_bin_aggregate_type == "avg":
                avg_dimension = pick([d for d in sample_json["tables"]["fact"]["fields"] if (d["type"] == "quantitative")])
                per_bin_aggregate["dimension"] = avg_dimension["field"]

            VizAction.VIZ_COUNTER += 1
            self.viz_name = "viz_%s" % VizAction.VIZ_COUNTER
            self.binning = [d_bin]
            self.perBinAggregates = [per_bin_aggregate]
            res = Operation(OrderedDict({"name": self.viz_name, "binning": self.binning, "perBinAggregates": self.perBinAggregates}))
            vizgraph.apply_interaction(res)
            states.append(res.data)
        # make links between all viz
        for i in range(VizAction.VIZ_COUNTER + 1):
            for j in range(i + 1, VizAction.VIZ_COUNTER + 1):
                LinkAction.LINKS.add((i, j))
                LinkAction.LINKS.add((j, i))
                incoming_links = ["viz_" + str(l[0]) for l in filter(lambda x: x[1] == i, LinkAction.LINKS)]
                combined_filters = Operation(
                    OrderedDict({"name": "viz_" + str(i), "source": (" and ".join(incoming_links))}))
                vizgraph.apply_interaction(combined_filters)
                incoming_links = ["viz_" + str(l[0]) for l in filter(lambda x: x[1] == j, LinkAction.LINKS)]
                combined_filters = Operation(
                    OrderedDict({"name": "viz_" + str(j), "source": (" and ".join(incoming_links))}))
                vizgraph.apply_interaction(combined_filters)

    # session
    def generate_session(self, df, schema, sample_json):
        tries = -1
        num_ops = 0
        num_query = 0
        states = []
        sqls = []
        vizgraph = VizGraph()
        self.init_viz(vizgraph, states, df, schema, sample_json)

        current = SelectionAction(self.options, self.config, df, vizgraph, schema, sample_json)
        while num_ops < self.options.num_operations:
            random.seed(self.options.seed + tries)
            np.random.seed(seed=self.options.seed + tries)
            # print(num_ops)
            if num_query >= self.options.session:
                vizgraph = self.reset_session(vizgraph)
                self.init_viz(vizgraph, states, df, schema, sample_json)
                current = SelectionAction(self.options, self.config, df, vizgraph, schema, sample_json)
                num_query = 0

            res = current.get_states()

            if res:
                affected_vizs = vizgraph.apply_interaction(res)
                for viz in affected_vizs:
                    tries += 1
                    # print(num_query)
                    if num_query >= self.options.session:
                        break
                    operation = Operation(OrderedDict({"name": ("%s" % viz.name), "filter": viz.computed_filter}))
                    states.append(operation.data)
                    sqls.append(viz.get_computed_filter_as_sql(schema))

                    num_query += 1
                    num_ops += 1

                    if num_ops % self.options.session == 0:
                        print('session %d done.' % (num_ops / self.options.session))

            current = current.get_next()

        with open("data/" + self.options.data_folder + "/workflows/" + self.options.path + ".txt", "w") as f:
            for sql in sqls:
                f.write(sql)
                f.write('\n')
            f.close()

        return states

    def init_viz(self, vizgraph, states, df, schema, sample_json):
        # VizAction
        for i in range(self.options.viz_number):
            viz = VizAction(self.options, self.config, df, vizgraph, schema, sample_json)
            res = viz.get_states()
            vizgraph.apply_interaction(res)
            states.append(res.data)
        # LinkAction
        link = LinkAction(self.options, self.config, df, vizgraph, schema, sample_json)
        res = link.get_states()
        while res:
            vizgraph.apply_interaction(res)
            # states.append(res.data)
            link = LinkAction(self.options, self.config, df, vizgraph, schema, sample_json)
            res = link.get_states()

    def reset_session(self, vizgraph):
        vizgraph = VizGraph()
        VizAction.VIZ_COUNTER = -1
        LinkAction.FIRST_LINK = None
        LinkAction.LATEST_LINK = None
        LinkAction.LINKS = set()

        return vizgraph

WorkflowGenerator()