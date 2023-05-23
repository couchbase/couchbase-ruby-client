require "rspec"

RSpec.shared_examples "a collection query index manager" do
  describe "#get_all_indexes" do
    let(:index_names) { [uniq_id(:foo), uniq_id(:bar)] }

    let(:result) do
      index_names.each { |idx_name| manager.create_index(idx_name, ["foo"]) }
      manager.get_all_indexes
    end

    it "the result is a non-empty array" do
      expect(result).to be_a(Array)
      expect(result.length).to be > 0
    end

    it "the result contains the known indexes" do
      expect(result.map(&:name)).to include(*index_names)
    end
  end

  describe "#create_index" do
    let(:index_name) { uniq_id(:test_index) }
    let(:deferred_index_name) { uniq_id(:deferred_test_index) }

    let(:result) do
      manager.create_index(index_name, ["field1"])
      def_opts = Couchbase::Management::Options::Query::CreateIndex.new(deferred: true)
      manager.create_index(deferred_index_name, ["field2"], def_opts)
      manager.get_all_indexes
    end

    it "the two indexes have been added" do
      expect(result.map(&:name)).to include(index_name, deferred_index_name)
    end

    it "the second index is in 'deferred' state" do
      expect(result.find { |idx| idx.name == deferred_index_name }.state).to be :deferred
    end
  end

  describe "#create_primary_index" do
    let(:result) do
      manager.create_primary_index
      manager.get_all_indexes
    end

    it "has created the primary index" do
      primary_index = result.find { |idx| idx.name == "#primary" }
      expect(primary_index.is_primary).to be true
    end
  end

  describe "#drop_index" do
    let(:index_name) { uniq_id(:test_drop_index) }

    let(:result) do
      manager.create_index(index_name, ["something"])
      manager.drop_index(index_name)
      manager.get_all_indexes
    end

    it "the index no longer exists" do
      expect(result.find { |idx| idx.name == index_name }).to be_nil
    end
  end

  describe "#build_deferred_indexes" do
    let!(:index_name) do
      name = uniq_id(:test_build_deferred)
      def_opts = Couchbase::Management::Options::Query::CreateIndex.new(deferred: true)
      manager.create_index(name, ["something"], def_opts)
      name
    end

    it "the index that was deferred is now online" do
      expect(manager.get_all_indexes.find { |idx| idx.name == index_name }.state).to be :deferred
      manager.build_deferred_indexes
      manager.watch_indexes([index_name], 5000)
      expect(manager.get_all_indexes.find { |idx| idx.name == index_name }.state).to be :online
    end
  end

  describe "#watch_indexes" do
    context "when the index exists" do
      let!(:index_name) do
        name = uniq_id(:test_build_deferred)
        def_opts = Couchbase::Management::Options::Query::CreateIndex.new(deferred: true)
        manager.create_index(name, ["something"], def_opts)
        name
      end

      it "returns once the index is online" do
        manager.build_deferred_indexes
        manager.watch_indexes([index_name], 5000)
        expect(manager.get_all_indexes.find { |idx| idx.name == index_name }.state).to be :online
      end
    end

    context "when the index does not exist" do
      it "raises an IndexNotFound error" do
        blk = proc { manager.watch_indexes([uniq_id(:non_existent_index)], 5000) }
        expect(&blk).to raise_error(Couchbase::Error::IndexNotFound)
      end
    end
  end
end
