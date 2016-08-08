package de.zalando.paradox.nakadi.consumer.core.http.handlers;

import com.fasterxml.jackson.databind.ObjectMapper;

import de.zalando.paradox.nakadi.consumer.core.DefaultObjectMapper;
import de.zalando.paradox.nakadi.consumer.core.domain.EventType;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;

class TestEvents {

    static final EventTypePartition EVENT_TYPE_PARTITION = EventTypePartition.of(EventType.of("order.ORDER_RECEIVED"),
            "0");

    static final ObjectMapper OBJECT_MAPPER = new DefaultObjectMapper().jacksonObjectMapper();

    //J-
    static final String ONE_EVENT =
        "{\"cursor\":" +
                "{" +
                    "\"partition\":\"0\"," +
                    "\"offset\":\"5\"" +
                "}," +
                "\"events\":" +
                    "[" +
                        "{" +
                            "\"metadata\":" +
                                "{" +
                                    "\"occurred_at\":\"2016-03-15T23:47:15+01:00\"," +
                                    "\"eid\":\"d765de34-09c0-4bbb-8b1e-7160a33a0791\"," +
                                    "\"event_type\":\"order.ORDER_RECEIVED\"," +
                                    "\"received_at\":\"2016-07-14T08:34:39.932Z\"," +
                                    "\"flow_id\":\"TnPBnBGSCIk7lqMcyCTPQoqb\"" +
                                 "}," +
                            "\"order_number\":\"24873243241\"" +
                         "}" +
                    "]" +
        "}\n";
    //J+

    //J-
    static final String ONE_EVENT_1 =
        "{\"metadata\":" +
                "{" +
                    "\"occurred_at\":\"2016-03-15T23:47:15+01:00\"," +
                    "\"eid\":\"d765de34-09c0-4bbb-8b1e-7160a33a0791\","+
                    "\"event_type\":\"order.ORDER_RECEIVED\"," +
                    "\"received_at\":\"2016-07-14T08:34:39.932Z\","+
                    "\"flow_id\":\"TnPBnBGSCIk7lqMcyCTPQoqb\"" +
                "}," +
            "\"order_number\":\"24873243241\"}";

    //J+

    //J-
    static final String TWO_EVENTS =
        "{\"cursor\":" +
                "{" +
                    "\"partition\":\"0\"," +
                    "\"offset\":\"9\"" +
                "}," +
                "\"events\":" +
                "[" +
                    "{" +
                        "\"metadata\":" +
                        "{" +
                            "\"occurred_at\":\"2016-03-15T23:47:15+01:00\"," +
                            "\"eid\":\"d765de34-09c0-4bbb-8b1e-7160a33a0791\"," +
                            "\"event_type\":\"order.ORDER_RECEIVED\"," +
                            "\"received_at\":\"2016-07-14T08:43:59.898Z\"," +
                            "\"flow_id\":\"u2aEbhUsU6lNWZSseJZNCg6b\"" +
                         "}," +
                        "\"order_number\":\"24873243241\"" +
                        "}" +
                    "," +
                    "{" +
                        "\"metadata\":" +
                        "{" +
                            "\"occurred_at\":\"2016-03-15T23:47:16+01:00\"," +
                            "\"eid\":\"a7671c51-49d1-48e6-bb03-b50dcf14f3d3\"," +
                            "\"event_type\":\"order.ORDER_RECEIVED\"," +
                            "\"received_at\":\"2016-07-14T08:43:59.898Z\"," +
                            "\"flow_id\":\"u2aEbhUsU6lNWZSseJZNCg6b\"" +
                        "}," +
                        "\"order_number\":\"24873243242\"" +
                        "}" +
                "]" +
        "}\n";

    //J+

    //J-
    static final String TWO_EVENTS_1 =
            "{\"metadata\":" +
                    "{" +
                    "\"occurred_at\":\"2016-03-15T23:47:15+01:00\"," +
                    "\"eid\":\"d765de34-09c0-4bbb-8b1e-7160a33a0791\","+
                    "\"event_type\":\"order.ORDER_RECEIVED\"," +
                    "\"received_at\":\"2016-07-14T08:43:59.898Z\","+
                    "\"flow_id\":\"u2aEbhUsU6lNWZSseJZNCg6b\"" +
                    "}," +
                    "\"order_number\":\"24873243241\"}";


    static final String TWO_EVENTS_2 =
            "{\"metadata\":" +
                    "{" +
                    "\"occurred_at\":\"2016-03-15T23:47:16+01:00\"," +
                    "\"eid\":\"a7671c51-49d1-48e6-bb03-b50dcf14f3d3\","+
                    "\"event_type\":\"order.ORDER_RECEIVED\"," +
                    "\"received_at\":\"2016-07-14T08:43:59.898Z\","+
                    "\"flow_id\":\"u2aEbhUsU6lNWZSseJZNCg6b\"" +
                    "}," +
                    "\"order_number\":\"24873243242\"}";

    //J+

    //J-
    static final String THREE_EVENTS =
            "{\"cursor\":" +
                    "{" +
                        "\"partition\":\"0\"," +
                        "\"offset\":\"9\"" +
                    "}," +
                    "\"events\":" +
                    "[" +
                        "{" +
                            "\"metadata\":" +
                            "{" +
                                "\"occurred_at\":\"2016-03-15T23:47:15+01:00\"," +
                                "\"eid\":\"d765de34-09c0-4bbb-8b1e-7160a33a0791\"," +
                                "\"event_type\":\"order.ORDER_RECEIVED\"," +
                                "\"received_at\":\"2016-07-14T08:43:59.898Z\"," +
                                "\"flow_id\":\"u2aEbhUsU6lNWZSseJZNCg6b\"" +
                             "}," +
                             "\"order_number\":\"24873243241\"" +
                        "}" +
                        "," +
                        "{" +
                            "\"metadata\":" +
                            "{" +
                                "\"occurred_at\":\"2016-03-15T23:47:16+01:00\"," +
                                "\"eid\":\"a7671c51-49d1-48e6-bb03-b50dcf14f3d3\"," +
                                "\"event_type\":\"order.ORDER_RECEIVED\"," +
                                "\"received_at\":\"2016-07-14T08:43:59.898Z\"," +
                                "\"flow_id\":\"u2aEbhUsU6lNWZSseJZNCg6b\"" +
                            "}," +
                            "\"order_number\":\"24873243242\"" +
                        "}" +
                        "," +
                        "{" +
                            "\"metadata\":" +
                                "{" +
                                "\"occurred_at\":\"2016-03-15T23:47:16+01:00\"," +
                                "\"eid\":\"a7671c51-49d1-48e6-bb03-b50dcf14f3d3\"," +
                                "\"event_type\":\"order.ORDER_RECEIVED\"," +
                                "\"received_at\":\"2016-07-14T08:43:59.898Z\"," +
                                "\"flow_id\":\"u2aEbhUsU6lNWZSseJZNCg6b\"" +
                                "}," +
                            "\"order_number\":\"24873243243\"" +
                        "}" +
                        "]" +
                    "}\n";
    //J+

    static final String KEEP_ALIVE_EVENT = "{\"cursor\":{\"partition\":\"0\",\"offset\":\"9\"}}";
}
