require "rspec"

RSpec.shared_examples "a cluster" do
  include_examples "cluster-level queries"
end
