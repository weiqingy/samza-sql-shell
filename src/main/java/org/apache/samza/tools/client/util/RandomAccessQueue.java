package org.apache.samza.tools.client.util;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;


public class RandomAccessQueue<T> {
  private T[] m_buffer;
  private int m_capacity;
  private int m_size;
  private int m_head;

  public RandomAccessQueue(Class<T> t, int capacity) {
    m_capacity = capacity;
    m_head = 0;
    m_size = 0;

    @SuppressWarnings("unchecked")
    final T[] buffer =  (T[]) Array.newInstance(t, capacity);
    m_buffer = buffer;
  }

  public List<T> get(int start, int end) {
    int lowerBound = Math.max(start, 0);
    int upperBound = Math.min(end, m_size - 1);
    List<T> rets = new ArrayList<>();
    for (int i = lowerBound; i <= upperBound; i++) {
      rets.add(m_buffer[(m_head + i) % m_capacity]);
    }
    return rets;
  }

  public T get(int index) {
    if (index >= 0 && index < m_size) {
      return m_buffer[(m_head + index) % m_capacity];
    }
    throw new CliException("OutOfBoundaryError");
  }

  public void add(T t) {
    if (m_size < m_capacity) {
      m_buffer[m_size] = t;
      m_size++;
    } else {
      m_buffer[m_head] = t;
      m_head = (m_head + 1) % m_capacity;
    }
  }

  /**
   * Remove all element before 'end', and return elements between 'start' and 'end'
   */
   public List<T> consume(int start, int end) {
     List<T> rets = get(start, end);
     m_head = (end + 1) % m_capacity;
     m_size -= end + 1;
     return rets;
   }

  public int getHead() {
    return m_head;
  }

  public int getSize() {
    return m_size;
  }

  public void clear() {
    m_head = 0;
    m_size = 0;
  }
}
