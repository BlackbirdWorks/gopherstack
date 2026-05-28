package elasticache

// ----------------------------------------
// Log delivery config XML (gap #9)
// ----------------------------------------

type logDeliveryDestinationDetailsXML struct {
	LogGroup       string `xml:"CloudWatchLogsDetails>LogGroup,omitempty"`
	DeliveryStream string `xml:"KinesisFirehoseDetails>DeliveryStream,omitempty"`
}

type logDeliveryConfigXML struct {
	DestinationDetails logDeliveryDestinationDetailsXML `xml:"DestinationDetails"`
	LogType            string                           `xml:"LogType,omitempty"`
	DestinationType    string                           `xml:"DestinationType,omitempty"`
	LogFormat          string                           `xml:"LogFormat,omitempty"`
	Status             string                           `xml:"Status,omitempty"`
	Message            string                           `xml:"Message,omitempty"`
}

type logDeliveryConfigsXML struct {
	LogDeliveryConfiguration []logDeliveryConfigXML `xml:"LogDeliveryConfiguration"`
}

func logDeliveryConfigsToXML(configs []LogDeliveryConfig) *logDeliveryConfigsXML {
	if len(configs) == 0 {
		return nil
	}

	items := make([]logDeliveryConfigXML, 0, len(configs))

	for _, c := range configs {
		item := logDeliveryConfigXML{
			LogType:         c.LogType,
			DestinationType: c.DestinationType,
			LogFormat:       c.LogFormat,
			Status:          c.Status,
			Message:         c.Message,
		}

		switch c.DestinationType {
		case "kinesis-firehose":
			item.DestinationDetails = logDeliveryDestinationDetailsXML{
				DeliveryStream: c.DestinationDetails,
			}
		default:
			item.DestinationDetails = logDeliveryDestinationDetailsXML{
				LogGroup: c.DestinationDetails,
			}
		}

		items = append(items, item)
	}

	return &logDeliveryConfigsXML{LogDeliveryConfiguration: items}
}
