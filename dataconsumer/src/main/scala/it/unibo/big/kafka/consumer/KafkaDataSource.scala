package it.unibo.big.kafka.consumer

import it.unibo.big.input.RecordModeling.Record
import it.unibo.big.input.reader.ReaderUtils.recordWithoutSeedConverter
import it.unibo.big.kafka.KafkaConfiguration
import it.unibo.big.query.app.DatasetsUtils._
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.slf4j.{Logger, LoggerFactory}

import java.time.Duration
import scala.jdk.CollectionConverters.iterableAsScalaIterableConverter

/**
 * Kafka data source that reads from the stream_generator topic
 */
class KafkaDataSource(kafkaConfiguration: KafkaConfiguration) extends Iterator[Record] {
  private val consumer = KafkaConsumerRecords.createConsumer(kafkaConfiguration)
  private val TIMEOUT_MILLS = 100

  private var buffer: Iterator[Record] = Iterator.empty
  private val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)
  private val lock = new Object //synchronization lock

  /**
   * Check if there are more records in the buffer
   * @return true if there are more records, false otherwise
   */
  override def hasNext: Boolean = lock.synchronized {
    if (!buffer.hasNext) {
      val records: ConsumerRecords[String, String] = consumer.poll(Duration.ofMillis(TIMEOUT_MILLS))
      val recordsSeq = records.asScala.map(line => {
        val dataset : Dataset = line.key().toLowerCase match { //TODO see if to add other version of datasets
          case "bitbang" => BitBang1
          case "welaser" => AgriRobot
          case x if x.startsWith("synthetic") =>
            //parse the string to obtain impact and extension
            //consider a string like synthetic_impactX_exetensionY
            val splittedString = x.split("_")
            if(splittedString.length > 1) {
              val impact = splittedString(1).toDouble
              val extension = splittedString(2).toDouble
              ChangingSyntheticDataset(impact, extension)
            } else {
              Synthetic("full_sim")
            }
        }
        (recordWithoutSeedConverter.parse(line.value()).setDataset(dataset), line.offset())
      })
      if(recordsSeq.nonEmpty) {
        val maxOffsetDataset = recordsSeq.maxBy(_._2)._1.dataset
        LOGGER.info(s"Consider dataset $maxOffsetDataset")
        buffer = recordsSeq.filter(_._1.dataset == maxOffsetDataset).map(_._1).toIterator
      }
    }
    buffer.hasNext
  }

  /**
   *
   * @return the next record
   */
  override def next(): Record  = lock.synchronized {
    if (hasNext) buffer.next()
    else throw new NoSuchElementException("No more elements in Kafka DataSource")
  }

  /**
   * Close the consumer
   */
  def close(): Unit = lock.synchronized {
    consumer.close()
  }
}
