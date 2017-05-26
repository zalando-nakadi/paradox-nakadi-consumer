package de.zalando.paradox.nakadi.consumer.boot;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;

import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import static org.mockito.MockitoAnnotations.initMocks;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup;

import java.util.Collections;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;

import org.hamcrest.Matchers;

import org.junit.Before;
import org.junit.Test;

import org.mockito.Mock;

import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;

import org.springframework.test.web.servlet.MockMvc;

import com.fasterxml.jackson.databind.ObjectMapper;

public class FailedEventReplayControllerTest {

    private MockMvc mockMvc;

    @Mock
    private FailedEventReplayer failedEventReplayer;

    @Before
    public void setUp() throws Exception {
        initMocks(this);
        mockMvc = standaloneSetup(new ControllerConfiguration.FailedEventReplayController(failedEventReplayer))
                .setMessageConverters(new MappingJackson2HttpMessageConverter(new ObjectMapper())).build();
    }

    @Test
    public void testShouldVerifyFailedEventSourceNames() throws Exception {
        final String failedEventSourceName = randomAlphabetic(10);
        when(failedEventReplayer.getFailedEventSources()).thenReturn(Collections.singletonList(failedEventSourceName));

        mockMvc.perform(get("/nakadi/failed-event-sources/")).andExpect(status().isOk()).andExpect(jsonPath(
                "$.failedEventSourceNames", Matchers.contains(failedEventSourceName)));
    }

    @Test
    public void testShouldReturnApproximatelyTotalNumberOfFailedEvents() throws Exception {
        final String failedEventSourceName = randomAlphabetic(10);
        final int approximatelyTotalNumberOfFailedEvents = RandomUtils.nextInt(1, 10);
        when(failedEventReplayer.getApproximatelyTotalNumberOfFailedEvents(failedEventSourceName)).thenReturn((long)
            approximatelyTotalNumberOfFailedEvents);

        mockMvc.perform(get("/nakadi/failed-event-sources/" + failedEventSourceName)).andExpect(status().isOk())
               .andExpect(jsonPath("$.approximatelyTotalNumberOfFailedEvents").value(
                       approximatelyTotalNumberOfFailedEvents));
    }

    @Test
    public void testShouldReplayFailedEvents() throws Exception {
        final String failedEventSourceName = randomAlphabetic(10);
        final String numberOfFailedEvents = RandomStringUtils.randomNumeric(2);

        doThrow(IllegalStateException.class).when(failedEventReplayer).replay(anyString(), anyLong(), anyBoolean());

        mockMvc.perform(post("/nakadi/failed-event-sources/" + failedEventSourceName).param("number_of_failed_events",
                       numberOfFailedEvents).param("break_processing_on_exception", "false")).andExpect(status()
                       .isBadRequest());
    }
}
