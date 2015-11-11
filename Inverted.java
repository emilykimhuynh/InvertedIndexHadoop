/*
*
* Emily Huynh
*
* CS 491: Clouds
* 11/20/2015
* Assignment 5
* MapReduce implementation of Inverted Index using Hadoop
*
*/

package org.apache.hadoop.examples;
import java.io.*;
import java.util.*;
import java.net.URI;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Inverted {

    //variable for extra word argument if necessary
    public static String findWord;

    //Begin InvertedMapper class
    public static class InvertedMapper extends Mapper<LongWritable, Text, Text, Text> {

        //private variables for InvertedMapper
        private Text word = new Text();
        private Text wordData = new Text();

        //map function
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            
                //grab each line
                String line = value.toString();
                String[] parts = line.split(" ");

                //get the document number
                String docno = parts[0].substring(0, 1);

                //emit each term in the line
                for(int i=1; i<parts.length; i++) {

                    //set the initial count to 1
                    int count = 1;

                    //check for duplicate words per document
                    for(int j=1; j<i; j++) {
                        if(parts[i].equals(parts[j]))
                            count++;
                    }

                    //result is (<docno>,<count>)
                    String result = "(" + docno + "," + count + ")";

                    //set key-value pair
                    word.set(parts[i]);
                    wordData.set(result);

                    //emit key-value pair to reduce
                    context.write(word, wordData);

                }//end for
        
        }// end map

    }//end Mapper class


    //begin Reducer class
    public static class InvertedReducer extends Reducer<Text, Text, Text, Text> {

        //private variables
        private Text result = new Text();
        private String wordData;
        private boolean needFind;

        //reduce function
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
           
            //if there is no findWord, proceed as usual
            if(findWord == null)
                needFind = false;

            //otherwise, set the condition
            else
                needFind = true;

            //structure of tuples
            ArrayList<String> sorted = new ArrayList<String>();
            // System.out.println("in reduce");

            //initialize the variables for document frequency, word, and tuples
            int df = 0;
            String word = key.toString();
            String tuples = "";

            //proceed as usual, write entire index
            if(!needFind) {

                //for each tuple, add it to the list of tuples
                for(Text val : values) {
                    sorted.add(val.toString());
                    df++;
                }//end for

                //sort the tuples
                Collections.sort(sorted);

                //prepare them for writing
                for(int i=0; i<sorted.size(); i++)
                    tuples += ", " + sorted.get(i);

                //format the output
                wordData = ": "  + df + " :" + tuples.substring(1);
                
                //set the key-value pairs
                key.set(word);
                result.set(wordData);

                //write everything to output
                context.write(key,result);
            }

            //write only the word we want
            else {

                //only write if the word is equal
                if(word.equals(findWord)) {

                    //for each tuple, add it to list of tuples
                    for(Text val : values) {
                        sorted.add(val.toString());
                        df++;
                    }//end for

                    //sort the tuples
                    Collections.sort(sorted);

                    //prepare them for writing
                    for(int i=0; i<sorted.size(); i++)
                        tuples += ", " + sorted.get(i);

                    //format the output
                    wordData = ": "  + df + " :" + tuples.substring(1);
                    
                    //set the key-value pairs
                    key.set(word);
                    result.set(wordData);

                    //write the single posting to output
                    context.write(key,result);

                }//end if

            }//end else

        }//end reduce

    }//end Reducer class

    //method to run the interactive interface
    public static void runInterface(FileSystem fs) throws IOException {
        
        //initializations 
        Scanner keyboard = new Scanner(System.in);
        boolean runProgram = true;

        //run the program indefinitely
        while(runProgram) {

            //prompt the user
            System.out.println("1) To search for a word, type the word and press 'enter'.");
            System.out.println("2) To see the entire index, press * and 'enter'.");
            System.out.println("3) To exit the program, type 'exit' and press 'enter'.");
            System.out.print("What do you want to do? ");
            String textIn = keyboard.nextLine();

            //exit the program
            if(textIn.equals("exit")) {

                //exit the while loop
                runProgram = false;
            }

            //print the entire index
            else if(textIn.equals("*")) {

                System.out.println("Now printing the entire index.\n");
                
                //set up the path and reader
                Path op = new Path("hdfs://localhost:54310/user/hduser/outputii/part-r-00000");
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(op)));
                
                //grab the first line
                String line;
                line = br.readLine();

                //print each line
                while (line != null) {
                    System.out.println(line);
                    line = br.readLine();
                }

                //close the BufferedReader
                br.close();
               
            }

            //search for a word in the index
            else {

                System.out.println("Searching for word: " + textIn + "\n");

                //set up the path and reader
                Path op = new Path("hdfs://localhost:54310/user/hduser/outputii/part-r-00000");
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(op)));
                
                //grab the first line
                boolean foundLine = false;
                String line;
                line = br.readLine();

                //print each line
                while (line != null) {

                    //check to see if textIn matches key
                    if(line.split("\t")[0].equals(textIn)) {
                        System.out.println(line + "\n");
                        foundLine = true;
                        break;
                    }//end if

                    //not that one, try the next line
                    line = br.readLine();

                }//end while

                //close the BufferedReader
                br.close();

                //notify user if word not found
                if(!foundLine)
                    System.out.printf("Word '%s' not found in index.\n\n", textIn);
            
            }//end else

        }//end while  

        //close the keyboard
        keyboard.close();

    }//end runInterface


    //begin main function
    public static void main(String args[]) throws Exception {

        //set up configuration
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        //check number of arguments
        if(otherArgs.length == 3)
            findWord = otherArgs[2];

        //error checking: incorrect number of args
        else if (otherArgs.length > 3) {
            System.err.println("Usage: inverted <in> <out> <word>");
             System.exit(2);
        }

        //define paths
        Path input = new Path(otherArgs[0]);
        Path output = new Path(otherArgs[1]);

        //if the output exists, delete it
        if(fs.exists(output)) {
            fs.delete(output, true);
        }

        //Start MapReduce
        Job job = new Job(conf, "inverted");
        job.setJarByClass(Inverted.class);
        job.setMapperClass(InvertedMapper.class);
        job.setReducerClass(InvertedReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        //wait for the MapReduce job to finish
        job.waitForCompletion(true);

        System.out.println("\n\n\nMapReduce Complete.\n\n");

        //run the program
        runInterface(fs);

        //close the file system
        fs.close();

        //we're done!
        System.out.println("Exiting the program.\n");
        System.exit(0);

    }//end main
    
}//end class
