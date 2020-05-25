package com.niyanchun.watermark;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;

import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;

/**
 * Assign timestamp and watermark at Source Function Demo.
 *
 * @author NiYanchun
 **/
public class AssignAtSourceDemo {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    env.addSource(new CustomSource())
        .timeWindowAll(Time.seconds(5))
        .trigger(new CustomEventTimeTrigger())
        .process(new CustomProcessFunction())
        .print();

    env.execute();
  }


  public static class CustomSource extends RichSourceFunction<JSONObject> {

    @Override
    public void run(SourceContext<JSONObject> ctx) throws Exception {
      System.out.println("event in source:");
      getOutOfOrderEvents().forEach(e -> {
        System.out.println(e);
        long timestampInMills = ((DateTime) e.get("timestamp")).getMillis();
        ctx.collectWithTimestamp(e, timestampInMills);
        ctx.emitWatermark(new Watermark(timestampInMills));
      });
      System.out.println();

      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    @Override
    public void cancel() {

    }
  }


  /**
   * generate out of order events
   *
   * @return List<JSONObject>
   */
  private static List<JSONObject> getOutOfOrderEvents() {
    // 2020-05-24 12:00:00
    JSONObject event1 = new JSONObject().fluentPut("id", "event1")
        .fluentPut("timestamp", new DateTime(2020, 5, 24, 12, 0, 0));
    // 2020-05-24 12:00:01
    JSONObject event2 = new JSONObject().fluentPut("id", "event2")
        .fluentPut("timestamp", new DateTime(2020, 5, 24, 12, 0, 1));
    // 2020-05-24 12:00:03
    JSONObject event3 = new JSONObject().fluentPut("id", "event3")
        .fluentPut("timestamp", new DateTime(2020, 5, 24, 12, 0, 3));
    // 2020-05-24 12:00:04
    JSONObject event4 = new JSONObject().fluentPut("id", "event4")
        .fluentPut("timestamp", new DateTime(2020, 5, 24, 12, 0, 4));
    // 2020-05-24 12:00:05
    JSONObject event5 = new JSONObject().fluentPut("id", "event5")
        .fluentPut("timestamp", new DateTime(2020, 5, 24, 12, 0, 5));
    // 2020-05-24 12:00:06
    JSONObject event6 = new JSONObject().fluentPut("id", "event6")
        .fluentPut("timestamp", new DateTime(2020, 5, 24, 12, 0, 6));
    // 2020-05-24 12:00:07
    JSONObject event7 = new JSONObject().fluentPut("id", "event7")
        .fluentPut("timestamp", new DateTime(2020, 5, 24, 12, 0, 7));
    // 2020-05-24 12:00:08
    JSONObject event8 = new JSONObject().fluentPut("id", "event8")
        .fluentPut("timestamp", new DateTime(2020, 5, 24, 12, 0, 8));
    // 2020-05-24 12:00:09
    JSONObject event9 = new JSONObject().fluentPut("id", "event9")
        .fluentPut("timestamp", new DateTime(2020, 5, 24, 12, 0, 9));

    // 这里把消息打乱，模拟实际中的消息乱序
    // 真实的消息产生顺序是（根据时间戳）：event1, event2, event3, event4, event5, event6, event7, event8, event9
    // 打乱之后的消息顺序是：event1, event2, event4, event3, event5, event7, event6, event8, event9
    return Arrays.asList(event1, event2, event4, event5, event7, event3, event6, event8, event9);
  }

  public static class CustomProcessFunction extends ProcessAllWindowFunction<JSONObject, Object, TimeWindow> {

    @Override
    public void process(Context context, Iterable<JSONObject> elements, Collector<Object> out) throws Exception {
      TimeWindow window = context.window();
      Format sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      System.out.println(String.format("\nwindow{%s - %s}", sdf.format(window.getStart()), sdf.format(window.getEnd())));
      int count = 0;
      for (JSONObject element : elements) {
        System.out.println(element.getString("id"));
        count++;
      }
      System.out.println("Total:" + count);
    }
  }

  public static class CustomEventTimeTrigger extends Trigger<Object, TimeWindow> {
    private static final long serialVersionUID = 1L;

    private CustomEventTimeTrigger() {
    }

    @Override
    public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
      Format sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

      System.out.println("onElement -- event: " + ((JSONObject) element).getString("id") +
          "; window.maxTimestamp():" + sdf.format(window.maxTimestamp()) +
          "; ctx.getCurrentWatermark():" + sdf.format(ctx.getCurrentWatermark()));
      if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
        // if the watermark is already past the window fire immediately
        return TriggerResult.FIRE;
      } else {
        ctx.registerEventTimeTimer(window.maxTimestamp());
        return TriggerResult.CONTINUE;
      }
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
      Format sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
      System.out.println("onEventTime-- window.maxTimestamp():" + sdf.format(window.maxTimestamp()) +
          "; time:" + sdf.format(time));
      return time == window.maxTimestamp() ?
          TriggerResult.FIRE :
          TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
      return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
      ctx.deleteEventTimeTimer(window.maxTimestamp());
    }

    @Override
    public boolean canMerge() {
      return true;
    }

    @Override
    public void onMerge(TimeWindow window,
                        OnMergeContext ctx) {
      // only register a timer if the watermark is not yet past the end of the merged window
      // this is in line with the logic in onElement(). If the watermark is past the end of
      // the window onElement() will fire and setting a timer here would fire the window twice.
      long windowMaxTimestamp = window.maxTimestamp();
      if (windowMaxTimestamp > ctx.getCurrentWatermark()) {
        ctx.registerEventTimeTimer(windowMaxTimestamp);
      }
    }

    @Override
    public String toString() {
      return "EventTimeTrigger()";
    }

    /**
     * Creates an event-time trigger that fires once the watermark passes the end of the window.
     *
     * <p>Once the trigger fires all elements are discarded. Elements that arrive late immediately
     * trigger window evaluation with just this one element.
     */
    public static CustomEventTimeTrigger create() {
      return new CustomEventTimeTrigger();
    }
  }
}
