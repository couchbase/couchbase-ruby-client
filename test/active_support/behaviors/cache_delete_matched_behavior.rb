# frozen_string_literal: true

module CacheDeleteMatchedBehavior
  def test_delete_matched
    skip("#{name}: delete_matched is not stable on 6.x servers, version=#{env.server_version}") if use_caves? || env.server_version.mad_hatter?
    begin
      @cache.delete_matched("*")
    rescue NotImplementedError
      skip("The server does not support query service for #delete_matched, real_server=#{use_server?}, #{env.server_version}")
    end
    @cache.write("foo", "bar")
    @cache.write("fu", "baz")
    @cache.write("foo/bar", "baz")
    @cache.write("fu/baz", "bar")
    deleted = @cache.delete_matched(/oo/)
    assert deleted >= 2
    sleep(0.3) while @cache.exist?("foo") || @cache.exist?("foo/bar") # HACK: to ensure that query changes have been propagated
    assert_not @cache.exist?("foo")
    assert @cache.exist?("fu")
    assert_not @cache.exist?("foo/bar")
    assert @cache.exist?("fu/baz")
  end
end
