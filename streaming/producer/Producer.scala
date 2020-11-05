import scalaj.http.Http
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import kafka.producer.KeyedMessage
import java.util.{Date, Properties}

import org.json4s._, org.json4s.native.JsonMethods._

object CitibikeProducer extends App {
    case class Data (
        stations: Seq[Stations]
    )

    case class EightdActiveStationServices (
        id: String
    )
 
    case class RootInterface (
        data: Data,
        last_updated: Int,
        ttl: Int
    )
 
    case class Stations (
        station_id: String,
        is_installed: Int,
        num_ebikes_available: Int,
        num_bikes_disabled: Int,
        num_docks_disabled: Int,
        station_status: String,
        is_returning: Int,
        eightd_has_available_keys: Boolean,
        legacy_id: String,
        num_docks_available: Int,
        num_bikes_available: Int,
        is_renting: Int,
        last_reported: Int,
        valet: Option[Valet],
        eightd_active_station_services: Option[Seq[EightdActiveStationServices]]
    )

    case class Valet (
        station_id: String,
        off_dock_capacity: Int,
        off_dock_count: Int,
		active: Boolean,
        valet_revision: Int,
        dock_blocked_count: Int,
        region: String
    )
 
	implicit val formats = DefaultFormats

	val topic = "bikes"
	val brokers = "localhost:9092"

	val props = new Properties()
	props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
	props.put(ProducerConfig.CLIENT_ID_CONFIG, "CitibikeProducer")
	props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
	props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

	val producer = new KafkaProducer[String, String](props)

	while(true) {
		val json = Http("https://gbfs.citibikenyc.com/gbfs/en/station_status.json").asString.body
		val parsedJson = parse(json)
		val stations = parsedJson.extract[RootInterface].data.stations
		for (station <- stations) {
			val data = new ProducerRecord[String, String](topic, null, station.station_id + "," + station.num_bikes_available)
			print(data + "\n")
			producer.send(data)
		}
		Thread.sleep(5000)
	}

	producer.close()
}
