import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PalabrasPositivas {

    // Palabras positivas permitidas
    private static final Set<String> PALABRAS_POSITIVAS =
        new HashSet<String>(Arrays.asList(
            "BUENO",
            "BUENISIMO",
            "EXCELENTE",
            "GENIAL",
            "FANTASTICO",
            "MAGNIFICA",
            "PERFECTA",
            "INCREIBLE",
            "RECOMENDADO"
        ));

    // =====================================================
    // MAPPER
    // =====================================================

    public static class PositivasMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable UNO =
                new IntWritable(1);

        private Text palabra = new Text();

        @Override
        public void map(
                Object key,
                Text value,
                Context context)
                throws IOException, InterruptedException {

            // Convertir toda la línea a mayúsculas
            String linea = value.toString().toUpperCase();

            // Separar las palabras
            String[] palabras =
                    linea.split("[^A-ZÁÉÍÓÚÑ]+");

            // Revisar cada palabra
            for (String p : palabras) {

                // Solo aceptar palabras positivas
                if (PALABRAS_POSITIVAS.contains(p)) {

                    palabra.set(p);

                    // Emitir PALABRA -> 1
                    context.write(palabra, UNO);
                }
            }
        }
    }

    // =====================================================
    // REDUCER
    // =====================================================

    public static class PositivasReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable resultado =
                new IntWritable();

        @Override
        public void reduce(
                Text palabra,
                Iterable<IntWritable> valores,
                Context context)
                throws IOException, InterruptedException {

            int suma = 0;

            // Sumar todas las apariciones
            for (IntWritable valor : valores) {
                suma += valor.get();
            }

            resultado.set(suma);

            // Emitir PALABRA -> TOTAL
            context.write(palabra, resultado);
        }
    }

    // =====================================================
    // MAIN
    // =====================================================

    public static void main(String[] args)
            throws Exception {

        if (args.length != 2) {

            System.err.println(
                "Uso: PalabrasPositivas <entrada> <salida>"
            );

            System.exit(-1);
        }

        Configuration configuracion =
                new Configuration();

        Job trabajo =
                Job.getInstance(
                    configuracion,
                    "Conteo de palabras positivas"
                );

        trabajo.setJarByClass(
                PalabrasPositivas.class
        );

        // Configurar Mapper
        trabajo.setMapperClass(
                PositivasMapper.class
        );

        // Configurar Reducer
        trabajo.setReducerClass(
                PositivasReducer.class
        );

        // Tipo de clave de salida
        trabajo.setOutputKeyClass(
                Text.class
        );

        // Tipo de valor de salida
        trabajo.setOutputValueClass(
                IntWritable.class
        );

        // Ruta de entrada
        FileInputFormat.addInputPath(
                trabajo,
                new Path(args[0])
        );

        // Ruta de salida
        FileOutputFormat.setOutputPath(
                trabajo,
                new Path(args[1])
        );

        // Ejecutar MapReduce
        System.exit(
            trabajo.waitForCompletion(true)
                ? 0
                : 1
        );
    }
}