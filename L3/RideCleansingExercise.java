package com.ververica.flinktraining.exercises.datastream_java.basics;

import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import com.ververica.flinktraining.exercises.datastream_java.utils.MissingSolutionException;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class RideCleansingExercise extends ExerciseBase {

    public static void main(String[] args) throws Exception {
        // Считывание аргументов командной строки
        ParameterTool parameters = ParameterTool.fromArgs(args);
        final String inputPath = parameters.get("input", ExerciseBase.pathToRideData);

        final int maxDelay = 60;            // Максимальная задержка событий — 60 секунд
        final int speedFactor = 600;        // 10 минут воспроизводятся за 1 секунду

        // Инициализация среды выполнения Flink
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(ExerciseBase.parallelism);

        // Генерация потока поездок
        DataStream<TaxiRide> rideStream = env.addSource(
                rideSourceOrTest(new TaxiRideSource(inputPath, maxDelay, speedFactor))
        );

        // Фильтрация поездок, начинающихся и заканчивающихся в NYC
        DataStream<TaxiRide> nycRides = rideStream
                .filter(new RideFilter());

        // Вывод отфильтрованных поездок
        printOrTest(nycRides);

        // Запуск Flink-приложения
        env.execute("Taxi Ride Cleansing");
    }

    // Класс фильтрации поездок в пределах Нью-Йорка
    private static class RideFilter implements FilterFunction<TaxiRide> {
        @Override
        public boolean filter(TaxiRide ride) throws Exception {
            // Проверка, начинается и заканчивается ли поездка в пределах NYC
            return ExerciseBase.isInNYC(ride.startLon, ride.startLat) &&
                    ExerciseBase.isInNYC(ride.endLon, ride.endLat);
        }
    }
}
