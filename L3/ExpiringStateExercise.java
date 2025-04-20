package com.ververica.flinktraining.exercises.datastream_java.process;

import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class ExpiringStateExercise extends ExerciseBase {

  // Побочные выходы: несопоставленные поездки и оплаты
  static final OutputTag<TaxiRide> unmatchedRides = new OutputTag<TaxiRide>("unmatchedRides") {};
  static final OutputTag<TaxiFare> unmatchedFares = new OutputTag<TaxiFare>("unmatchedFares") {};

  public static void main(String[] args) throws Exception {

    ParameterTool params = ParameterTool.fromArgs(args);
    final String ridesFile = params.get("rides", ExerciseBase.pathToRideData);
    final String faresFile = params.get("fares", ExerciseBase.pathToFareData);

    final int maxEventDelay = 60;            // максимальная задержка событий
    final int servingSpeedFactor = 600;      // ускорение подачи данных

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); // используем событийное время
    env.setParallelism(ExerciseBase.parallelism); // устанавливаем уровень параллелизма

    // Источник данных о поездках
    DataStream<TaxiRide> rides = env
            .addSource(rideSourceOrTest(new TaxiRideSource(ridesFile, maxEventDelay, servingSpeedFactor)))
            .filter(ride -> ride.isStart && ride.rideId % 1000 != 0) // фильтруем только старты поездок, исключаем rideId кратные 1000
            .keyBy(ride -> ride.rideId); // группировка по rideId

    // Источник данных об оплатах
    DataStream<TaxiFare> fares = env
            .addSource(fareSourceOrTest(new TaxiFareSource(faresFile, maxEventDelay, servingSpeedFactor)))
            .keyBy(fare -> fare.rideId); // группировка по rideId

    // Объединение и обогащение поездок и оплат
    SingleOutputStreamOperator<Tuple2<TaxiRide, TaxiFare>> processed = rides
            .connect(fares)
            .process(new EnrichmentFunction());

    // Печать побочного потока с несопоставленными оплатами
    printOrTest(processed.getSideOutput(unmatchedFares));

    env.execute("ExpiringStateSolution (java)");
  }


  public static class EnrichmentFunction
          extends KeyedCoProcessFunction<Long, TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {

    private ValueState<TaxiRide> rideState; // Состояние для хранения поездки
    private ValueState<TaxiFare> fareState; // Состояние для хранения оплаты

    @Override
    public void open(Configuration config) {
      // Инициализация состояний
      rideState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved ride", TaxiRide.class));
      fareState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved fare", TaxiFare.class));
    }

    @Override
    public void processElement1(
            TaxiRide ride,
            Context context,
            Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {

      TaxiFare fare = fareState.value();

      if (fare != null) {
        fareState.clear(); // удаляем оплату из состояния
        context.timerService().deleteEventTimeTimer(fare.getEventTime()); // отмена таймера
        out.collect(new Tuple2<>(ride, fare)); // сопоставляем и выводим
      } else {
        rideState.update(ride); // сохраняем поездку
        context.timerService().registerEventTimeTimer(ride.getEventTime()); // регистрируем таймер
      }
    }
  }
}
