
package anagrams;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Anagram extends Configured implements Tool {

    public static class AnagramMapper extends Mapper<LongWritable, Text, Text, Text> {
        
        private Text sortedText = new Text();
        private Text orginalText = new Text();
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            
            //Convertir les mots en chaîne de caractères et les stocker dans un tableau de caractères
            String word = value.toString();
	    String word1 = word.replace(" ","");
            char[] wordChars = word1.toCharArray();
            //On va trier les mots par ordre alphabétique, Une fois triés les mots anagrames seront égaux
            Arrays.sort(wordChars);
            //Convertir le tableau de caractères triés en type chaîne
            String sortedWord = new String(wordChars);
            sortedText.set(sortedWord);
            //Stocker les caractères originaux dans originalText
            orginalText.set(word);
            
            context.write(sortedText, orginalText);
        }
    }
    
    public static class AnagramReducer extends Reducer<Text, Text, Text, Text> {
        
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //Utiliser le symbole «~» pour lier des mots composés des mêmes lettres (les anagrames)
            String output = "";
            for(Text anagram:values) {
                if (!output.equals("")) {
                    output = output + "~";
                }
                output += anagram.toString();
            }
            
            StringTokenizer outputTokenizer = new StringTokenizer(output, "~");
            //Filtrer les mots avec une seule lettre
            if (outputTokenizer.countTokens() >= 2) {
                output = output.replaceAll("~", ",");
                
                context.write(key, new Text(output));
            }
        }
    }
    
    
    public static void main(String[] args) throws Exception {

        String[] args0 = {args[0], args[1]};
        int ec = ToolRunner.run(new Configuration(), new Anagram(), args0);//Exécuter la valeur de retour
        System.exit(ec);
        
    }

    @Override
    public int run(String[] arg0) throws Exception {
       
        
        //Charger le fichier de configuration
        Configuration conf = new Configuration();
        
        //Déterminer si le chemin de sortie existe, supprimez-le s'il existe
        Path myPath = new Path(arg0[1]);
        FileSystem hdfs = myPath.getFileSystem(conf);
        if (hdfs.isDirectory(myPath)) {
            hdfs.delete(myPath, true);
        }
        
        //Construire un objet Job
        Job job = new Job(conf, "anagram");
        
        //Définir la classe principale
        job.setJarByClass(Anagram.class);
        
        //Définir le chemin d'entrée et de sortie
        FileInputFormat.addInputPath(job, new Path(arg0[0]));
        FileOutputFormat.setOutputPath(job, new Path(arg0[1]));

        // Appeler  notre mapper et notre reducer
        job.setMapperClass(AnagramMapper.class);
        job.setReducerClass(AnagramReducer.class);

        //Définir les types de clé et de valeur
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        //Valider le job
        job.waitForCompletion(true);
        
        return 0;
    }

}