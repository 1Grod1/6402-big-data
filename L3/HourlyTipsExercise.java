package com.ververica.flinktraining.exercises.datastream_java.windows;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class HourlyTipsExercise extends ExerciseBase {

  public static void main(String[] args) throws Exception {

    // Получаем параметры из командной строки
    ParameterTool params = ParameterTool.fromArgs(args);
    final String input = params.get("input", ExerciseBase.pathToFareData);
    final int maxEventDelay = 60; // максимальная задержка событий (сек)
    final int servingSpeedFactor = 600; // ускорение подачи данных

    // Создание среды исполнения
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); // Используем событийное время
    env.setParallelism(ExerciseBase.parallelism); // Установка параллелизма

    // Источник данных — информация о поездках и чаевых
    DataStream<TaxiFare> fares = env.addSource(
            fareSourceOrTest(new TaxiFareSource(input, maxEventDelay, servingSpeedFactor)));

    // Подсчёт суммы чаевых по каждому водителю за каждый час
    DataStream<Tuple3<Long, Long, Float>> hourlyMax = fares
            .keyBy(fare -> fare.driverId) // группировка по ID водителя
            .timeWindow(Time.hours(1)) // оконная агрегация за 1 час
            .process(new AddTips()) // подсчёт чаевых в окне
            .timeWindowAll(Time.hours(1)) // глобальное окно для сравнения всех водителей
            .maxBy(2); // выбираем водителя с максимальной суммой чаевых (по индексу 2 в кортеже)
    
    printOrTest(hourlyMax);

    // Запуск выполнения
    env.execute("Hourly Tips (java)");
  }


  public static class AddTips extends ProcessWindowFunction<
          TaxiFare,                    // Входной тип — объект TaxiFare
          Tuple3<Long, Long, Float>,  // Выходной тип — кортеж (время, ID водителя, сумма чаевых)
          Long,                       // Ключ (ID водителя)
          TimeWindow> {

    @Override
    public void process(
            Long driverId,
            Context context,
            Iterable<TaxiFare> fares,
            Collector<Tuple3<Long, Long, Float>> out) throws Exception {

      float sumOfTips = 0F; // сумма чаевых

      // Суммируем чаевые в окне
      for (TaxiFare fare : fares) {
        sumOfTips += fare.tip;
      }

      // Отправляем кортеж: время конца окна, ID водителя, сумма чаевых
      out.collect(new Tuple3<>(context.window().getEnd(), driverId, sumOfTips));
    }
  }
}
