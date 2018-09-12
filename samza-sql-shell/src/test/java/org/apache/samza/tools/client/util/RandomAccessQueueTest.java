package org.apache.samza.tools.client.util;

import java.util.List;
import org.junit.Assert;
import org.junit.Test;


public class RandomAccessQueueTest {
  private RandomAccessQueue m_queue;
  public RandomAccessQueueTest() {
    m_queue = new RandomAccessQueue<>(Integer.class, 5);
  }

  @Test
  public void testAddAndGetElement() {
    m_queue.clear();
    for (int i = 0; i < 4; i++) {
      m_queue.add(i);
    }
    Assert.assertEquals(0, m_queue.getHead());
    Assert.assertEquals(4, m_queue.getSize());
    Assert.assertEquals(0, m_queue.get(0));
    Assert.assertEquals(3, m_queue.get(3));

    for (int i = 0; i < 3; i++) {
      m_queue.add(4 + i);
    }
    int head = m_queue.getHead();
    Assert.assertEquals(2, head);
    Assert.assertEquals(5, m_queue.getSize());
    Assert.assertEquals(2, m_queue.get(0));
    Assert.assertEquals(3, m_queue.get(1));
    Assert.assertEquals(4, m_queue.get(2));
    Assert.assertEquals(5, m_queue.get(3));
    Assert.assertEquals(6, m_queue.get(4));
  }

  @Test
  public void testGetRange() {
    m_queue.clear();
    for (int i = 0; i < 4; i++) {
      m_queue.add(i); // 0, 1, 2, 3
    }
    List<Integer> rets = m_queue.get(-1, 9);
    Assert.assertEquals(4, rets.size());
    Assert.assertEquals(0, m_queue.get(0));
    Assert.assertEquals(3, m_queue.get(3));

    for (int i = 0; i <= 2; i++) {
      m_queue.add(4 + i);
    }
    rets = m_queue.get(0, 4);
    Assert.assertEquals(2, rets.get(0).intValue());
    Assert.assertEquals(3, rets.get(1).intValue());
    Assert.assertEquals(4, rets.get(2).intValue());
    Assert.assertEquals(5, rets.get(3).intValue());
    Assert.assertEquals(6, rets.get(4).intValue());
  }

  @Test
  public void testConsume() {
    m_queue.clear();
    for (int i = 0; i < 4; i++) {
      m_queue.add(i); // 0, 1, 2, 3
    }
    List<Integer> rets = m_queue.consume(1, 2);
    Assert.assertEquals(1, m_queue.getSize());
    Assert.assertEquals(3, m_queue.getHead());
  }
}