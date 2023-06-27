/**
  * StructuredStreamSpec:
  *
  * Run coverage report with sbt using command:
  * sbt ';coverageEnabled;test'
  */
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Outcome}
import org.scalatest.funsuite.AnyFunSuite

import io.github.sandeep_sandhu.stream_analytics._

// Enables setting up fixtures using before-all and after-all
class StructuredStreamSpec
    extends AnyFunSuite
    with BeforeAndAfterEach
    with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    // TODO: use this for one-time costly fixture setup
  }

  override def afterAll(): Unit = {
    // TODO: use this to close the one-time costly fixture
  }

  test("A basic dataframe creation test") {
    // TODO: use mock producer and kafka queue for testing
    assert(1 == 1)
  }
}

trait SparkSessionTestWrapper {}
