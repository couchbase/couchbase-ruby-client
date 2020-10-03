# frozen_string_literal: true

module CacheDeleteMatchedBehavior
  def test_delete_matched

    @cache.write("foo", "bar")
    @cache.write("fu", "baz")
    @cache.write("foo/bar", "baz")
    @cache.write("fu/baz", "bar")
    if use_server? && env.server_version.supports_regexp_matches?
      @cache.delete_matched(/oo/)
      assert_not @cache.exist?("foo")
      assert @cache.exist?("fu")
      assert_not @cache.exist?("foo/bar")
      assert @cache.exist?("fu/baz")
    else
      assert_raises NotImplementedError do
        @cache.delete_matched(/oo/)
      end
    end
  end
end
