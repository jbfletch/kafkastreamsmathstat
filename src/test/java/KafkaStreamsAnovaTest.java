
import com.bakdata.fluent_kafka_streams_tests.junit5.TestTopologyExtension;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.jayway.jsonpath.internal.filter.ValueNodes;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import serde.JsonSerde;
import serde.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import serde.JsonSerializer;

import java.io.IOException;
import java.util.ArrayList;


public class KafkaStreamsAnovaTest {

    private final Serde<JsonNode> jsonSerde = new JsonSerde();
    private final KafkaStreamsAnova app = new KafkaStreamsAnova();
    private final ObjectMapper mapper = new ObjectMapper()
        .configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true)
        .configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);

    @RegisterExtension
    final TestTopologyExtension<Object, JsonNode> testTopology =
        new TestTopologyExtension<Object, JsonNode>(this.app::getTopology, this.app.getKafkaProperties());

    @Test
    void steamingClassMappingTest()
        throws IOException {
        JsonNode obs1 = mapper.readValue("{'sneaker' : '42','peeps':'3'}",JsonNode.class);
        JsonNode obs2 = mapper.readValue("{'sneaker' : '18','peeps':'2'}",JsonNode.class);
        JsonNode obs3 = mapper.readValue("{'sneaker' : '42','peeps':'17'}",JsonNode.class);
        JsonNode obs4 = mapper.readValue("{'sneaker' : '42','peeps':'5'}",JsonNode.class);
        JsonNode obs5 = mapper.readValue("{'sneaker' : '18','peeps':'7'}",JsonNode.class);
        JsonNode obs6 = mapper.readValue("{'sneaker' : '18','peeps':'4'}",JsonNode.class);
        JsonNode obs7 = mapper.readValue("{'sneaker' : '18','peeps':'92'}",JsonNode.class);
        JsonNode obs8 = mapper.readValue("{'sneaker' : '18','peeps':'1200'}",JsonNode.class);
        JsonNode obs9 = mapper.readValue("{'sneaker' : '18','peeps':'5'}",JsonNode.class);
        JsonNode obs10 = mapper.readValue("{'sneaker' : '42','peeps':'3'}",JsonNode.class);
        JsonNode obs11 = mapper.readValue("{'sneaker' : '18','peeps':'2'}",JsonNode.class);
        JsonNode obs13 = mapper.readValue("{'sneaker' : '42','peeps':'17'}",JsonNode.class);
        JsonNode obs14 = mapper.readValue("{'sneaker' : '42','peeps':'5'}",JsonNode.class);
        JsonNode obs15 = mapper.readValue("{'sneaker' : '18','peeps':'7'}",JsonNode.class);
        JsonNode obs16 = mapper.readValue("{'sneaker' : '18','peeps':'4'}",JsonNode.class);


        this.testTopology.input()
            .at(1000).add("1",obs1)
            .at(1100).add("2",obs2)
            .at(1200).add("3",obs3)
            .at(1300).add("4",obs4)
            .at(1400).add("5",obs5)
            .at(1500).add("6",obs6)
            .at(1510).add("10",obs10)
            .at(1520).add("11",obs11)
            .at(1530).add("13",obs13)
            .at(1540).add("14",obs14)
            .at(1550).add("15",obs15)
            .at(1560).add("16",obs16)
            .at(12000).add("7",obs7)
            .at(12100).add("8",obs8)
            .at(12200).add("9",obs9);
        this.testTopology.streamOutput("updates-by-class").withSerde(Serdes.String(),Serdes.ListSerde(ArrayList.class,Serdes.Integer()));

    }

    @Test
    void steamingDataMappingTest()
        throws IOException {
        JsonNode obs1 = mapper.readValue("{'sneaker' : '42','peeps':'3'}",JsonNode.class);
        JsonNode obs2 = mapper.readValue("{'sneaker' : '18','peeps':'2'}",JsonNode.class);
        JsonNode obs3 = mapper.readValue("{'sneaker' : '42','peeps':'17'}",JsonNode.class);
        JsonNode obs4 = mapper.readValue("{'sneaker' : '42','peeps':'5'}",JsonNode.class);
        JsonNode obs5 = mapper.readValue("{'sneaker' : '18','peeps':'7'}",JsonNode.class);
        JsonNode obs6 = mapper.readValue("{'sneaker' : '18','peeps':'4'}",JsonNode.class);
        JsonNode obs7 = mapper.readValue("{'sneaker' : '18','peeps':'92'}",JsonNode.class);
        JsonNode obs8 = mapper.readValue("{'sneaker' : '18','peeps':'1200'}",JsonNode.class);
        JsonNode obs9 = mapper.readValue("{'sneaker' : '18','peeps':'5'}",JsonNode.class);
        JsonNode obs10 = mapper.readValue("{'sneaker' : '42','peeps':'3'}",JsonNode.class);
        JsonNode obs11 = mapper.readValue("{'sneaker' : '18','peeps':'2'}",JsonNode.class);
        JsonNode obs13 = mapper.readValue("{'sneaker' : '42','peeps':'17'}",JsonNode.class);
        JsonNode obs14 = mapper.readValue("{'sneaker' : '42','peeps':'5'}",JsonNode.class);
        JsonNode obs15 = mapper.readValue("{'sneaker' : '18','peeps':'7'}",JsonNode.class);
        JsonNode obs16 = mapper.readValue("{'sneaker' : '18','peeps':'4'}",JsonNode.class);


        this.testTopology.input()
            .at(1000).add("x",obs1)
            .at(1100).add("x",obs2)
            .at(1200).add("x",obs3)
            .at(1300).add("x",obs4)
            .at(1400).add("x",obs5)
            .at(1500).add("x",obs6)
            .at(1510).add("x",obs10)
            .at(1520).add("x",obs11)
            .at(1530).add("x",obs13)
            .at(1540).add("x",obs14)
            .at(1550).add("x",obs15)
            .at(1560).add("x",obs16)
            .at(12000).add("x",obs7)
            .at(12100).add("x",obs8)
            .at(12200).add("x",obs9);
        this.testTopology.streamOutput("updates-full").withSerde(Serdes.String(),Serdes.ListSerde(ArrayList.class,Serdes.Integer()));

    }

}
