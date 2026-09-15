import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OrdenarPositivas {

    // =====================================================
    // MAPPER
    // =====================================================

    public static class OrdenarMapper
            extends Mapper<Object, Text, IntWritable, Text> {

        private IntWritable frecuencia =
                new IntWritable();

        private Text palabra =
                new Text();

        @Override
        public void map(
                Object key,
                Text value,
                Context context)
                throws IOException, InterruptedException {

            // Recibir una línea:
            // PALABRA    FRECUENCIA

            String linea = value.toString().trim();

            // Separar palabra y frecuencia
            String[] partes =
                    linea.split("\\s+");

            if (partes.length == 2) {

                palabra.set(partes[0]);

                int cantidad =
                        Integer.parseInt(partes[1]);

                frecuencia.set(cantidad);

                // FRECUENCIA -> PALABRA
                context.write(
                        frecuencia,
                        palabra
                );
            }
        }
    }

    // =====================================================
    // REDUCER
    // =====================================================

    public static class OrdenarReducer
            extends Reducer<IntWritable, Text, Text, IntWritable> {

        @Override
        public void reduce(
                IntWritable frecuencia,
                Iterable<Text> palabras,
                Context context)
                throws IOException, InterruptedException {

            for (Text palabra : palabras) {

                // PALABRA -> FRECUENCIA
                context.write(
                        palabra,
                        frecuencia
                );
            }
        }
    }

    // =====================================================
    // MAIN
    // =====================================================

    public static void main(String[] args)
            throws Exception {

        if (args.length != 2) {

            System.err.println(
                "Uso: OrdenarPositivas <entrada> <salida>"
            );

            System.exit(-1);
        }

        Configuration configuracion =
                new Configuration();

        Job trabajo =
                Job.getInstance(
                    configuracion,
                    "Ordenamiento Top Down"
                );

        trabajo.setJarByClass(
                OrdenarPositivas.class
        );

        // Mapper
        trabajo.setMapperClass(
                OrdenarMapper.class
        );

        // Reducer
        trabajo.setReducerClass(
                OrdenarReducer.class
        );

        // Tipos de salida del Mapper
        trabajo.setMapOutputKeyClass(
                IntWritable.class
        );

        trabajo.setMapOutputValueClass(
                Text.class
        );

        // Tipos de salida final
        trabajo.setOutputKeyClass(
                Text.class
        );

        trabajo.setOutputValueClass(
                IntWritable.class
        );

        // Entrada
        FileInputFormat.addInputPath(
                trabajo,
                new Path(args[0])
        );

        // Salida
        FileOutputFormat.setOutputPath(
                trabajo,
                new Path(args[1])
        );

        System.exit(
            trabajo.waitForCompletion(true)
                ? 0
                : 1
        );
    }
}