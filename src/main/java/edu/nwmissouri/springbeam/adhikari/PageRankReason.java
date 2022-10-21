/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.nwmissouri.springbeam.adhikari;

import java.io.File;
import java.util.*;
import java.util.Collection;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileSystemUtils;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptors;





public class PageRankReason {
    // DEFINE DOFNS
  // ==================================================================
  // You can make your pipeline assembly code less verbose by defining
  // your DoFns statically out-of-line.
  // Each DoFn<InputT, OutputT> takes previous output
  // as input of type InputT
  // and transforms it to OutputT.
  // We pass this DoFn to a ParDo in our pipeline.

  
  static class Job1Finalizer extends DoFn<KV<String, Iterable<String>>, KV<String, RankedPage>> {
    @ProcessElement
    public void processElement(@Element KV<String, Iterable<String>> element,
        OutputReceiver<KV<String, RankedPage>> receiver) {
      Integer contributorVotes = 0;
      if (element.getValue() instanceof Collection) {
        contributorVotes = ((Collection<String>) element.getValue()).size();
      }
      ArrayList<VotingPage> voters = new ArrayList<VotingPage>();
      for (String voterName : element.getValue()) {
        if (!voterName.isEmpty()) {
          voters.add(new VotingPage(voterName, contributorVotes));
        }
      }
      receiver.output(KV.of(element.getKey(), new RankedPage(element.getKey(), voters)));
    }
  }

  static class Job2Mapper extends DoFn<KV<String, RankedPage>, KV<String, RankedPage>> {
    @ProcessElement
    public void processElement(@Element KV<String, RankedPage> element,
        OutputReceiver<KV<String, RankedPage>> receiver) {
      Integer NumOfVotes=0;
      ArrayList<VotingPage> voters = element.getValue().getVoters();

      if (element.getValue().getVoters() instanceof Collection){
        NumOfVotes = ((Collection<VotingPage>)element.getValue().getVoters()).size();
      }

      for(VotingPage page: voters){
        String pageName = page.getNames();
        Double pageRank = page.getRank();
        String contributingPageName = element.getKey();
        Double contributingPageRank = element.getValue().getRank();
        VotingPage contributor = new VotingPage(contributingPageName, contributingPageRank, NumOfVotes);
        ArrayList<VotingPage> votingPageArr = new ArrayList<VotingPage>();
        votingPageArr.add(contributor);
        receiver.output(KV.of(page.getNames(), new RankedPage(pageName, pageRank,votingPageArr)));
      }
      
    }
  }

  static class Job2Updater extends DoFn<KV<String, Iterable<RankedPage>>, KV<String, RankedPage>> {
    @ProcessElement
    public void processElement(@Element KV<String, Iterable<RankedPage>> element,
        OutputReceiver<KV<String, RankedPage>> receiver) {
    String page = element.getKey();
    Iterable<RankedPage> rankedPages = element.getValue();
    Double dampingFactor = 0.85;
    Double updatedRank = (1-dampingFactor);
    ArrayList<VotingPage> newVoters = new ArrayList<VotingPage>();
    for(RankedPage pg : rankedPages){
      if(pg != null){
        for(VotingPage vPage : pg.getVoters()){
          newVoters.add(vPage);
          updatedRank += (dampingFactor) * vPage.getRank() / (double)vPage.getVotes();
        }
      }
    }
    receiver.output(KV.of(page, new RankedPage(page, updatedRank, newVoters)));
    }

  }

  public static PCollection<KV<String, String>> adhikariKVPairGenerator(Pipeline p, String folderName, String fileName) {

    String dataPath = "./" + folderName + "/" + fileName;
    PCollection<String> pColLine = p.apply(TextIO.read().from(dataPath));

    PCollection<String> pColLinkLine = pColLine.apply(Filter.by((String linkline) -> linkline.startsWith("[")))
        .apply(
            MapElements.into(TypeDescriptors.strings())
                .via((String linkline) -> linkline.strip()));

    PCollection<String> pColLinks = pColLinkLine.apply(
        MapElements.into(TypeDescriptors.strings())
            .via((String linkword) -> (findLink(linkword))));

    PCollection<KV<String, String>> pColKVPairs = pColLinks.apply(
        MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(),
            TypeDescriptors.strings()))
            .via(
                (String links) -> KV.of((String) fileName, (String) links)));

    return pColKVPairs;
  }

  public static String findLink(String line) {
    String link = "";
    int beginIndex = line.indexOf("(");
    int endIndex = line.indexOf(")");
    link = line.substring(beginIndex + 1, endIndex);
    return link;
  }
 

  static class mapToRankPage extends DoFn<KV<String, RankedPage>, KV<Double, String>> {
    @ProcessElement
    public void processElement(@Element KV<String, RankedPage> element,
        OutputReceiver<KV<Double, String>> receiver) {
    String pageName = element.getKey();
    Double pageRank = element.getValue().getRank();
    receiver.output(KV.of(pageRank, pageName));
    }
  }

  static class sortPages extends DoFn<KV<Double, String>, KV<Double, String>> {
    @ProcessElement
    public void processElement(@Element KV<Double, String> element,
        OutputReceiver<KV<Double, String>> receiver) {
    PCollectionList<KV<Double, String>> sortedValues = null;
    int min= 0;
    if(element.getKey()>min){
      min=1;
    }
    receiver.output(KV.of(null, null));
    }
  }

  private static void deleteOutputFiles(){
    final File dir = new File("./AdhikariOutput/");
    for (File f: dir.listFiles()){
      if (f.getName().startsWith("AdhikariPR")){
        f.delete();
      }
    }
  }
  public static void main(String[] args) {
    // Create a PipelineOptions object. This object lets us set various execution
    // options for our pipeline, such as the runner you wish to use. This example
    // will run with the DirectRunner by default, based on the class path configured
    // in its dependencies.
    PipelineOptions options = PipelineOptionsFactory.create();

    // Create the Pipeline object with the options we defined above
    Pipeline p = Pipeline.create(options);

    // Initiating the variable for web04 and file name
    String folderName = "web04";
    // String fileName = "go.md";

    //generating kv pairs for each webpage 
    PCollection<KV<String, String>> pColKV1 = adhikariKVPairGenerator(p, folderName, "go.md");
    PCollection<KV<String, String>> pColKV2 = adhikariKVPairGenerator(p, folderName, "java.md");
    PCollection<KV<String, String>> pColKV3 = adhikariKVPairGenerator(p, folderName, "python.md");
    PCollection<KV<String, String>> pColKV4 = adhikariKVPairGenerator(p, folderName, "README.md");
    
    //merging all kv pairs into one PCollectionList then PCollection
    PCollectionList<KV<String, String>> pColKVList = PCollectionList.of(pColKV1).and(pColKV2).and(pColKV3).and(pColKV4);
    PCollection<KV<String, String>> mergedList = pColKVList.apply(Flatten.<KV<String, String>>pCollections());

    PCollection<KV<String, Iterable<String>>> pColReduced =
     mergedList.apply(GroupByKey.<String, String>create());

    // Convert to a custom Value object (RankedPage) in preparation for Job 2
    PCollection<KV<String, RankedPage>> job1output = pColReduced.apply(ParDo.of(new Job1Finalizer()));


    //END OF JOB1
   
    PCollection<KV<String, RankedPage>> updatedOutput = null;
    PCollection<KV<String, RankedPage>> mappedKVs = null;

    int iterations =50;
    for (int i =0; i<iterations; i++){
      if(i==0){
        mappedKVs = job1output
          .apply(ParDo.of(new Job2Mapper()));
      }else{
        mappedKVs = updatedOutput
          .apply(ParDo.of(new Job2Mapper()));
      }      
      PCollection<KV<String, Iterable<RankedPage>>> reducedKVs = mappedKVs
        .apply(GroupByKey.<String, RankedPage>create());
      updatedOutput = reducedKVs.apply(ParDo.of(new Job2Updater()));
    }

    //JOB 3
    //mapping job2 output to rank-page KV pairs
    PCollection<KV<Double, String>> mappedToNameRank = updatedOutput.apply(ParDo.of(new mapToRankPage()));
    PCollectionList<KV<Double, String>> sortedPColList = PCollectionList.of(mappedToNameRank);
    PCollection<KV<Double, String>> sortedMerge = sortedPColList.apply(Flatten.pCollections());
   
    

    //Changing to be able to write using TextIO
    PCollection<String> writableFile = sortedMerge.apply(MapElements.into(TypeDescriptors.strings())
      .via((kvpairs) -> kvpairs.toString()));
    
    //Deleting output files before creating new files
    deleteOutputFiles();
    
    //writing the result
    writableFile.apply(TextIO.write().to("AdhikariOutput/AdhikariPR"));
    p.run().waitUntilFinish();
  }
}
