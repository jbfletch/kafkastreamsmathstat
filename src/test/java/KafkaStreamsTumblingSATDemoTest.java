import com.bakdata.fluent_kafka_streams_tests.junit5.TestTopologyExtension;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import serde.JsonSerde;

import java.io.IOException;
import java.util.ArrayList;

public class KafkaStreamsTumblingSATDemoTest {
    private final Serde<JsonNode> jsonSerde = new JsonSerde();
    private final KafkaStreamsTumblingSATDemo app = new KafkaStreamsTumblingSATDemo();
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
        JsonNode obs6 = mapper.readValue("{'sneaker' : '42','peeps':'4'}",JsonNode.class);
        JsonNode obs7 = mapper.readValue("{'sneaker' : '18','peeps':'92'}",JsonNode.class);
        JsonNode obs8 = mapper.readValue("{'sneaker' : '42','peeps':'1200'}",JsonNode.class);
        JsonNode obs9 = mapper.readValue("{'sneaker' : '18','peeps':'5'}",JsonNode.class);
        JsonNode obs10 = mapper.readValue("{'sneaker' : '42','peeps':'3'}",JsonNode.class);
        JsonNode obs11 = mapper.readValue("{'sneaker' : '42','peeps':'2'}",JsonNode.class);
        JsonNode obs12 = mapper.readValue("{'sneaker' : '42','peeps':'3'}",JsonNode.class);
        JsonNode obs13 = mapper.readValue("{'sneaker' : '42','peeps':'17'}",JsonNode.class);
        JsonNode obs14 = mapper.readValue("{'sneaker' : '42','peeps':'5'}",JsonNode.class);
        JsonNode obs15 = mapper.readValue("{'sneaker' : '42','peeps':'7'}",JsonNode.class);
        JsonNode obs16 = mapper.readValue("{'sneaker' : '42','peeps':'4'}",JsonNode.class);
        JsonNode obs17 = mapper.readValue("{'sneaker' : '42','peeps':'3'}",JsonNode.class);
        JsonNode obs18 = mapper.readValue("{'sneaker' : '42','peeps':'2'}",JsonNode.class);
        JsonNode obs19 = mapper.readValue("{'sneaker' : '42','peeps':'17'}",JsonNode.class);
        JsonNode obs20 = mapper.readValue("{'sneaker' : '42','peeps':'5'}",JsonNode.class);
        JsonNode obs21 = mapper.readValue("{'sneaker' : '42','peeps':'7'}",JsonNode.class);
        JsonNode obs22 = mapper.readValue("{'sneaker' : '42','peeps':'4'}",JsonNode.class);
        JsonNode obs23 = mapper.readValue("{'sneaker' : '42','peeps':'92'}",JsonNode.class);
        JsonNode obs24 = mapper.readValue("{'sneaker' : '42','peeps':'1200'}",JsonNode.class);
        JsonNode obs25 = mapper.readValue("{'sneaker' : '18','peeps':'5'}",JsonNode.class);
        JsonNode obs26 = mapper.readValue("{'sneaker' : '42','peeps':'3'}",JsonNode.class);
        JsonNode obs27 = mapper.readValue("{'sneaker' : '42','peeps':'2'}",JsonNode.class);
        JsonNode obs28 = mapper.readValue("{'sneaker' : '42','peeps':'17'}",JsonNode.class);
        JsonNode obs29 = mapper.readValue("{'sneaker' : '42','peeps':'5'}",JsonNode.class);
        JsonNode obs30 = mapper.readValue("{'sneaker' : '18','peeps':'7'}",JsonNode.class);
        JsonNode obs31 = mapper.readValue("{'sneaker' : '18','peeps':'4'}",JsonNode.class);
        JsonNode obs32 = mapper.readValue("{'sneaker' : '42','peeps':'4'}",JsonNode.class);
        JsonNode obs33 = mapper.readValue("{'sneaker' : '42','peeps':'92'}",JsonNode.class);
        JsonNode obs34 = mapper.readValue("{'sneaker' : '42','peeps':'1200'}",JsonNode.class);
        JsonNode obs35 = mapper.readValue("{'sneaker' : '18','peeps':'5'}",JsonNode.class);
        JsonNode obs36 = mapper.readValue("{'sneaker' : '42','peeps':'3'}",JsonNode.class);
        JsonNode obs37 = mapper.readValue("{'sneaker' : '42','peeps':'2'}",JsonNode.class);
        JsonNode obs38 = mapper.readValue("{'sneaker' : '42','peeps':'17'}",JsonNode.class);
        JsonNode obs39 = mapper.readValue("{'sneaker' : '42','peeps':'5'}",JsonNode.class);
        JsonNode obs40 = mapper.readValue("{'sneaker' : '18','peeps':'7'}",JsonNode.class);
        JsonNode obs41 = mapper.readValue("{'sneaker' : '18','peeps':'4'}",JsonNode.class);


        this.testTopology.input()
            .at(1000).add("1",obs1)
            .at(1100).add("2",obs2)
            .at(1200).add("3",obs3)
            .at(1300).add("4",obs4)
            .at(1400).add("5",obs5)
            .at(1500).add("6",obs6)
            .at(1510).add("10",obs10)
            .at(1520).add("11",obs11)
            .at(1521).add("12",obs12)
            .at(1530).add("13",obs13)
            .at(1540).add("14",obs14)
            .at(1550).add("15",obs15)
            .at(1560).add("16",obs16)
            .at(1570).add("17",obs17)
            .at(1580).add("18",obs18)
            .at(1590).add("19",obs19)
            .at(1600).add("20",obs20)
            .at(1601).add("21",obs21)
            .at(1602).add("22",obs22)
            .at(1603).add("23",obs23)
            .at(1604).add("24",obs24)
            .at(1605).add("25",obs25)
            .at(1606).add("26",obs26)
            .at(1607).add("27",obs27)
            .at(1608).add("28",obs28)
            .at(1609).add("29",obs29)
            .at(1610).add("30",obs30)
            .at(1611).add("31",obs31)
            .at(1612).add("32",obs32)
            .at(1613).add("33",obs33)
            .at(1614).add("34",obs34)
            .at(1615).add("35",obs35)
            .at(1616).add("36",obs36)
            .at(1617).add("37",obs37)
            .at(1618).add("38",obs38)
            .at(1619).add("39",obs39)
            .at(1620).add("40",obs40)
            .at(1621).add("41",obs41)
//            .at(1622).add("42",obs13)
//            .at(1623).add("43",obs14)
//            .at(1624).add("44",obs15)
//            .at(1625).add("45",obs16)
//            .at(1626).add("46",obs17)
//            .at(1627).add("47",obs18)
//            .at(1628).add("33",obs19)
//            .at(1629).add("34",obs20)
//            .at(1630).add("35",obs21)
//            .at(1631).add("36",obs22)
//            .at(1632).add("37",obs23)
            .at(12000).add("7",obs7)
            .at(12100).add("8",obs8)
            .at(12200).add("9",obs9);
        this.testTopology.streamOutput("updates-by-class").withSerde(Serdes.String(),Serdes.ListSerde(ArrayList.class,Serdes.Integer()));

    }
}
