import random
import time
from collections import OrderedDict
from datetime import datetime

from common.operation import Operation
from workflowgen.baseaction import BaseAction
from workflowgen.linkaction import LinkAction
import pandasql
import math
import numpy as np
class SelectionAction(BaseAction):

    def get_states(self, pre):

        if len(LinkAction.LINKS) == 0:
            return

        rand_link = self.pick(list(LinkAction.LINKS))
        rand_link_src = rand_link[0]
        nodes_dict = self.vizgraph.get_nodes_dict()
        src_viz = nodes_dict["viz_" + str(rand_link_src)]
        computed_filter = src_viz.get_computed_filter()
        df = self.df
        sql_statement = "SELECT * FROM df "
        if len(computed_filter) > 0:
            sql_statement += "WHERE " + computed_filter

        df_result = pandasql.sqldf(sql_statement, locals())

        if df_result.empty:
            print('got None')
            for viz in nodes_dict:
                new_filter = 'AND'.join([f for f in nodes_dict[viz].computed_filter.split('AND') if f.find(pre.selection) == -1])
                nodes_dict[viz].computed_filter = new_filter
            pre.selection = ""
            print('reset '+pre.name)
            computed_filter = src_viz.get_computed_filter()
            sql_statement = "SELECT * FROM df "
            if len(computed_filter) > 0:
                sql_statement += "WHERE " + computed_filter
            df_result = pandasql.sqldf(sql_statement, locals())
            # return None

        filter_per_dim = []

        for bin_dim in range(len(src_viz.binning)):
            filters = []
            dim = src_viz.binning[bin_dim]["dimension"]
            field = list(filter(lambda x: x["field"] == dim, self.sample_json["tables"]["fact"]["fields"]))[0]
            if field["type"] == "quantitative":
                bin_width = float(src_viz.binning[bin_dim]["width"])
                all_min = df[dim].min()
                all_max = df[dim].max()
                cur_min = df_result[dim].min()
                cur_max = df_result[dim].max()
                if src_viz.selection == "":
                    print(src_viz.name + ' addBrush')
                    first_idx = random.randint(0, bin_width - 1)
                    last_idx = random.randint(first_idx, bin_width - 1)
                    selected_bins = (first_idx, last_idx + 1)
                else:
                    first_idx = (cur_min-all_min)//bin_width
                    last_idx = (cur_max-all_min)//bin_width
                    interaction = np.random.rand()
                    if interaction < 0.3:
                        print(src_viz.name + ' adjustBrushRange from left')
                        possible = random.randint(0, last_idx)
                        selected_bins = (possible, last_idx+1)
                    elif interaction < 0.6:
                        print(src_viz.name + ' dragBrush')
                        possible = random.randint(0, bin_width-(last_idx-first_idx+1))
                        selected_bins = (possible, possible+(last_idx-first_idx)+1)
                    else:
                        print(src_viz.name + ' adjustBrushRange from right')
                        possible = random.randint(first_idx, bin_width-1)
                        selected_bins = (first_idx, possible+1)
                range_min = selected_bins[0] * 10 + all_min
                range_max = selected_bins[1] * 10 + all_min
                filt = "(%s >= %s and %s < %s)" % (dim, '{:.1f}'.format(range_min), dim, '{:.1f}'.format(range_max))
                filters.append(filt)

                # min_val = df_result[dim].min()
                # max_val = df_result[dim].max()
                #
                # min_index = math.floor(min_val / bin_width)
                # max_index = math.floor(max_val / bin_width)
                # num_bins = 0
                # if np.random.rand() < 0.4:
                #     num_bins = 1
                # else:
                #     num_bins = random.randint(1, max_index-min_index) if max_index > min_index else 1
                # num_bins = num_bins if num_bins <= self.options.upbound else self.options.upbound
                # selected_bins = np.random.choice(np.arange(min_index, max_index + 1), size=num_bins, replace=False)
                #
                # for selected_bin in selected_bins:
                #     range_min = selected_bin * bin_width
                #     range_max = (selected_bin + 1) * bin_width
                #     filt = "(%s >= %s and %s < %s)" % (dim, '{:.1f}'.format(range_min), dim, '{:.1f}'.format(range_max))
                #     filters.append(filt)
            elif field["type"] == "categorical":
                all_bins = sorted(df[dim].unique().tolist())
                cur_bins = sorted(df_result[dim].unique().tolist())
                if src_viz.selection == "":
                    print(src_viz.name + ' addBrush')
                    first_idx = random.randint(0, len(all_bins)-1)
                    last_idx = random.randint(first_idx, len(all_bins)-1)
                    selected_bins = all_bins[first_idx:last_idx+1]
                else:
                    first_idx = all_bins.index(cur_bins[0])
                    last_idx = all_bins.index(cur_bins[-1])
                    interaction = np.random.rand()
                    if interaction < 0.3:
                        print(src_viz.name + ' adjustBrushRange from left')
                        possible = random.randint(0, last_idx)
                        selected_bins = all_bins[possible:last_idx+1]
                    elif interaction < 0.6:
                        print(src_viz.name + ' dragBrush')
                        possible = random.randint(0, len(all_bins)-len(cur_bins))
                        selected_bins = all_bins[possible: possible+len(cur_bins)]
                    else:
                        print(src_viz.name + ' adjustBrushRange from right')
                        possible = random.randint(first_idx, len(all_bins)-1)
                        selected_bins = all_bins[first_idx: possible+1]

                # num_bins = random.randint(1, len(all_bins))
                # num_bins = num_bins if num_bins <= self.options.upbound else self.options.upbound
                # selected_bins = np.random.choice(all_bins, size=num_bins, replace=False)
                # for selected_bin in list(selected_bins):
                #     filt =  "(%s = '%s')" % (dim, selected_bin)
                #     filters.append(filt)
                filters.append("%s IN (%s)" % (dim, ','.join(["'%s'" % x for x in selected_bins])))
            elif field["type"] == "temporal":
                all_bins = sorted(df[dim].unique().tolist())
                cur_bins = sorted(df_result[dim].unique().tolist())
                if src_viz.selection == "":
                    print(src_viz.name + ' addBrush')
                    first_idx = random.randint(0, len(all_bins)-1)
                    last_idx = random.randint(first_idx, len(all_bins)-1)
                    selected_bins = (first_idx, last_idx+1)
                else:
                    first_idx = all_bins.index(cur_bins[0])
                    last_idx = all_bins.index(cur_bins[-1])
                    interaction = np.random.rand()
                    if interaction < 0.3:
                        print(src_viz.name + ' adjustBrushRange from left')
                        possible = random.randint(0, last_idx)
                        selected_bins = (possible, last_idx+1)
                    elif interaction < 0.6:
                        print(src_viz.name + ' dragBrush')
                        possible = random.randint(0, len(all_bins)-len(cur_bins))
                        selected_bins = (possible, possible+len(cur_bins))
                    else:
                        print(src_viz.name + ' adjustBrushRange from right')
                        possible = random.randint(first_idx, len(all_bins)-1)
                        selected_bins = (first_idx, possible+1)
                format = "%Y-%m-%d %H:%M:%S"
                date1 = datetime.strptime(all_bins[selected_bins[0]][:-6], "%Y-%m-%d %H:%M:%S")
                date2 = datetime.strptime(all_bins[selected_bins[1]][:-6], "%Y-%m-%d %H:%M:%S")
                filters.append("%s BETWEEN %s and %s" % (dim, date1, date2))
            filter_per_dim.append(" or ".join(filters))
        filter_per_dim = ["%s" % f for f in filter_per_dim]

        return Operation(OrderedDict({"name": ("viz_%s" % rand_link_src), "selection": " AND ".join(filter_per_dim)}))
