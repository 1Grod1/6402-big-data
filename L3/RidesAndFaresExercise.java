package com.ververica.flinktraining.exercises.datastream_java.state;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

public class RidesAndFaresExercise extends ExerciseBase {

  public static void main(String[] args) throws Exception {
    // Получение аргументов командной строки
    ParameterTool params = ParameterTool.fromArgs(args);
    final String ridesPath = params.get("rides", pathToRideData);
    final String faresPath = params.get("fares", pathToFareData);

    final int maxEventDelay = 60;
    final int servingSpeed = 1800;

    // Конфигурация среды исполнения с включённой Web UI
    Configuration conf = new Configuration();
    conf.setString("state.backend", "filesystem");
    conf.setString("state.savepoints.dir", "file:///tmp/savepoints");
    conf.setString("state.checkpoints.dir", "file:///tmp/checkpoints");

    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
    env.setParallelism(parallelism); // Установка уровня параллелизма

    // Настройка чекпоинтов
    env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
    CheckpointConfig checkpointConfig = env.getCheckpointConfig();
    checkpointConfig.enableExternalizedCheckpoints(
            CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

    // Источник данных о поездках (берём только начало поездки)
    DataStream<TaxiRide> ridesStream = env
            .addSource(rideSourceOrTest(new TaxiRideSource(ridesPath, maxEventDelay, servingSpeed)))
            .filter(TaxiRide::isStart) // Фильтруем только начальные события поездок
            .keyBy(ride -> ride.rideId); // Группируем по rideId

    // Источник данных об оплатах
    DataStream<TaxiFare> faresStream = env
            .addSource(fareSourceOrTest(new TaxiFareSource(faresPath, maxEventDelay, servingSpeed)))
            .keyBy(fare -> fare.rideId); // Группируем по rideId

    // Объединение данных о поездке и её стоимости
    DataStream<Tuple2<TaxiRide, TaxiFare>> enrichedStream = ridesStream
            .connect(faresStream)
            .flatMap(new EnrichmentFunction()) // Обогащаем поездки данными об оплате
            .uid("ride-fare-enrichment");

    // Выводим объединённый поток
    printOrTest(enrichedStream);

    // Запуск выполнения
    env.execute("Join Taxi Rides with Fares (RichCoFlatMap)");
  }

  /**
   * Класс для объединения поездок и оплат, которые приходят в поток в разное время.
   */
  public static class EnrichmentFunction extends RichCoFlatMapFunction<TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {
    private ValueState<TaxiRide> rideState;
    private ValueState<TaxiFare> fareState;

    @Override
    public void open(Configuration config) {
      // Инициализация состояния для хранения поездок и оплат
      rideState = getRuntimeContext().getState(new ValueStateDescriptor<>("savedRide", TaxiRide.class));
      fareState = getRuntimeContext().getState(new ValueStateDescriptor<>("savedFare", TaxiFare.class));
    }

    // Обработка поступающей поездки
    @Override
    public void flatMap1(TaxiRide ride, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
      TaxiFare fare = fareState.value(); // Пытаемся найти соответствующую оплату
      if (fare != null) {
        fareState.clear(); // Очистка после объединения
        out.collect(new Tuple2<>(ride, fare));
      } else {
        rideState.update(ride); // Сохраняем поездку в ожидании оплаты
      }
    }

    // Обработка поступающей оплаты
    @Override
    public void flatMap2(TaxiFare fare, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
      TaxiRide ride = rideState.value(); // Пытаемся найти соответствующую поездку
      if (ride != null) {
        rideState.clear();
        out.collect(new Tuple2<>(ride, fare));
      } else {
        fareState.update(fare); // Сохраняем оплату в ожидании поездки
      }
    }
  }
}
