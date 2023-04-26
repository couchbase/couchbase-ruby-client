require "rspec"

RSpec.shared_examples "a scope" do
  describe "#query" do
    context "when performing a simple query" do
      before do
        scope = cluster.bucket("travel-sample").scope("inventory")
        statement = "SELECT * " \
                    "FROM airline " \
                    "WHERE id > 10000"
        @query_result = scope.query(statement, Couchbase::Options::Query.new(metrics: true))
        @query_result.transcoder = Couchbase::JsonTranscoder.new
      end

      it "the correct number of rows was returned" do
        expect(@query_result.rows.size).to be 47
      end

      it "the returned number of rows matches the `result_count` field in the metadata" do
        expect(@query_result.meta_data.metrics.result_count).to eq(@query_result.rows.size)
      end
    end
  end
end
