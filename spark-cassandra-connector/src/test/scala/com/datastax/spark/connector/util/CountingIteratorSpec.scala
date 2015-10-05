package com.datastax.spark.connector.util

import org.scalatest.{FlatSpec, Matchers}


class CountingIteratorSpec extends FlatSpec with Matchers {

  def seq100Iter()=  (1 to 100).iterator

  "CountingIterator" should "count consumed elements" in {
    val iterator = new CountingIterator(seq100Iter())
    while (iterator.hasNext) {
      iterator.next() shouldBe iterator.count
    }
  }

  it should "be initialized with zero consumed elements" in {
    val iterator = new CountingIterator(seq100Iter())
    iterator.count shouldBe 0
  }

  it should "limit the number of consumable elements" in {
    val limit = 5
    val iterator = new CountingIterator(seq100Iter(), Some(limit))
    iterator.size shouldBe limit
  }

  it should "deliver all elements within the limit" in {
    val iterator = new CountingIterator(seq100Iter(), Some(10))
    iterator.toSeq should contain theSameElementsAs(1 to 10)
  }


  it should "deliver all elements below the limit" in {
    val limit = seq100Iter().size + 1
    val iterator = new CountingIterator(seq100Iter(), Some(limit))
    iterator.size shouldBe(seq100Iter().size)
  }

  it should "honor empty underlying iterator" in {
    val iterator = new CountingIterator(Seq().toIterator, Some(1))
    iterator.size shouldBe(0)
  }

  it should "not haveNext if limit is zero" in {
    val iterator = new CountingIterator(seq100Iter(), Some(0))
    iterator.hasNext shouldBe false
  }

  it should "be empty if limit is zero" in {
    val iterator = new CountingIterator(seq100Iter(), Some(0))
    iterator.isEmpty shouldBe true
  }




}
