package glue

import (
	"strings"
)

const minimalPythonScript = `import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
glueContext = GlueContext(SparkContext.getOrCreate())
job = Job(glueContext)
job.commit()
`

const minimalScalaScript = `import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.util.Job
import org.apache.spark.SparkContext
object GlueApp {
  def main(sysArgs: Array[String]): Unit = {
    val sc: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(sc)
    Job.init("glue_job", glueContext, Map.empty)
    Job.commit()
  }
}
`

// GetPlan returns minimal ETL code appropriate for the requested language.
// language should be "Python" or "Scala"; defaults to Python.
func (b *InMemoryBackend) GetPlan(language string) (string, string) {
	switch strings.ToUpper(language) {
	case "SCALA":
		return "", minimalScalaScript
	default:
		return minimalPythonScript, ""
	}
}
