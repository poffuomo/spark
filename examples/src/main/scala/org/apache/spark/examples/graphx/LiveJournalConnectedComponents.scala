/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package org.apache.spark.examples.graphx

/**
 * Use GraphX to run the Connected Components algorithm on a `LiveJournal` graph.
 * Download the dataset from https://snap.stanford.edu/data/com-LiveJournal.html.
 *
 * @author poffuomo
 * @version 1.0 6/26/17
 */
object LiveJournalConnectedComponents {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println(
        "Usage: LiveJournalConnectedComponents <edge_list_file>\n" +
          "    --numEPart=<num_edge_partitions>\n" +
          "        The number of partitions for the graph's edge RDD.\n" +
          "    [--tol=<tolerance>]\n" +
          "        The tolerance allowed at convergence (smaller => more accurate). Default is " +
          "0.001.\n" +
          "    [--output=<output_file>]\n" +
          "        If specified, the file to write the ranks to.\n" +
          "    [--partStrategy=RandomVertexCut | EdgePartition1D | EdgePartition2D | " +
          "CanonicalRandomVertexCut]\n" +
          "        The way edges are assigned to edge partitions. Default is RandomVertexCut.")
      System.exit(-1)
    }

    Analytics.main(args.patch(0, List("cc"), 0))
  }
}
// scalastyle:on println
