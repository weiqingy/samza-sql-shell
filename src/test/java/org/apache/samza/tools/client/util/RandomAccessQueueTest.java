package org.apache.samza.tools.client.util;

import java.util.List;
import org.junit.Assert;
import org.junit.Test;


public class RandomAccessQueueTest {
  private RandomAccessQueue _queue;
  public RandomAccessQueueTest() {
    _queue = new RandomAccessQueue<>(Integer.class, 5);
  }

  @Test
  public void testAddAndGetElement() {
    _queue.clear();
    for (int i = 0; i < 4; i++) {
      _queue.add(i);
    }
    Assert.assertEquals(0, _queue.getHead());
    Assert.assertEquals(4, _queue.getSize());
    Assert.assertEquals(0, _queue.get(0));
    Assert.assertEquals(3, _queue.get(3));

    for (int i = 0; i < 3; i++) {
      _queue.add(4 + i);
    }
    int head = _queue.getHead();
    Assert.assertEquals(2, head);
    Assert.assertEquals(5, _queue.getSize());
    Assert.assertEquals(2, _queue.get(0));
    Assert.assertEquals(3, _queue.get(1));
    Assert.assertEquals(4, _queue.get(2));
    Assert.assertEquals(5, _queue.get(3));
    Assert.assertEquals(6, _queue.get(4));
  }

  @Test
  public void testGetRange() {
    _queue.clear();
    for (int i = 0; i < 4; i++) {
      _queue.add(i);
    }
    List<Integer> rets = _queue.get(-1, 9);
    Assert.assertEquals(4, rets.size());
    Assert.assertEquals(0, _queue.get(0));
    Assert.assertEquals(3, _queue.get(3));

    for (int i = 0; i < 3; i++) {
      _queue.add(4 + i);
    }
    rets = _queue.get(0, 4);
    Assert.assertEquals(2, rets.get(0).intValue());
    Assert.assertEquals(3, rets.get(1).intValue());
    Assert.assertEquals(4, rets.get(2).intValue());
    Assert.assertEquals(5, rets.get(3).intValue());
    Assert.assertEquals(6, rets.get(4).intValue());
  }

  @Test
  public void testConsume() {
    _queue.clear();
    for (int i = 0; i < 4; i++) {
      _queue.add(i); // 0, 1, 2, 3
    }
    List<Integer> rets = _queue.consume(1, 2);
    Assert.assertEquals(1, _queue.getSize());
    Assert.assertEquals(3, _queue.getHead());
  }
}