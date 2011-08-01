require File.join(File.dirname(__FILE__), 'setup')

class TestLatch < MiniTest::Unit::TestCase
    def test_properly_initialize
      l = Couchbase::Latch.new(true, false)
      assert_equal(true, l.state)
      assert_equal(false, l.target)
    end

    def test_basic_latch_usage
      latch = Couchbase::Latch.new(:dirty, :ready)
      obj = "bar"
      Thread.new do
        obj = "foo"
        latch.toggle
      end
      latch.wait
      assert_equal(:ready, latch.state)
      assert_equal("foo", obj)
    end

    def test_basic_latch_usage_inverted
      latch = Couchbase::Latch.new(:dirty, :ready)
      obj = "bar"
      Thread.new do
        latch.wait
        assert_equal(:ready, latch.state)
        assert_equal("foo", obj)
      end
      obj = "foo"
      latch.toggle
    end

    def test_latch_with_identical_values_skips_wait
      latch = Couchbase::Latch.new(:ready, :ready)
      latch.wait
      assert_equal(:ready, latch.state)
    end

    def test_toggle_with_two_chained_threads
      latch = Couchbase::Latch.new(:start, :end)
      name = "foo"
      Thread.new do
        Thread.new do
          name = "bar"
          latch.toggle
        end
      end
      latch.wait
      assert_equal(:end, latch.state)
      assert_equal("bar", name)
    end

    def test_toggle_with_multiple_waiters
      proceed_latch = Couchbase::Latch.new(:start, :end)
      result = :bar
      Thread.new do
        proceed_latch.wait
        assert_equal(:foo, result)
      end
      Thread.new do
        proceed_latch.wait
        assert_equal(:foo, result)
      end
      assert_equal(:bar, result)
      proceed_latch.toggle
      assert_equal(:end, proceed_latch.state)
    end

    def test_interleaved_latches
      check_latch = Couchbase::Latch.new(:dirty, :ready)
      change_1_latch = Couchbase::Latch.new(:dirty, :ready)
      change_2_latch = Couchbase::Latch.new(:dirty, :ready)
      name = "foo"
      Thread.new do
        name = "bar"
        change_1_latch.toggle
        check_latch.wait
        name = "man"
        change_2_latch.toggle
      end
      change_1_latch.wait
      assert_equal("bar", name)
      check_latch.toggle
      change_2_latch.wait
      assert_equal("man", name)
    end
end
