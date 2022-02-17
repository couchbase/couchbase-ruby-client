# frozen_string_literal: true

module CacheDeleteMatchedBehavior
  def test_delete_matched
    begin
      @cache.delete_matched("*")
    rescue NotImplementedError
      skip("The server does not support query service for #delete_matched, real_server=#{use_server?}, #{env.server_version}")
    end
    @cache.write("foo", "bar")
    @cache.write("fu", "baz")
    @cache.write("foo/bar", "baz")
    @cache.write("fu/baz", "bar")
    @cache.delete_matched(/oo/)
    sleep(0.3) while @cache.exist?("foo") # HACK: to ensure that query changes have been propagated
    assert_not @cache.exist?("foo")
    assert @cache.exist?("fu")
    assert_not @cache.exist?("foo/bar")
    assert @cache.exist?("fu/baz")
  end
end
