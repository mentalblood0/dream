require "sophia"

module Dream
  Sophia.define_env DreamEnv, {tags: {key: {tag: String,
                                            oid: String}}}

  class Index
    def initialize(path : String, opts : Sophia::H = Sophia::H{"compression"      => "zstd",
                                                               "compaction.cache" => 2_i64 * 1024 * 1024 * 1024})
      @sophia = DreamEnv.new Sophia::H{"sophia.path" => path}, {tags: opts}
    end

    def add(oid : String, tags : Array(String))
      @sophia << tags.map { |tag| {tag: tag, oid: oid} } unless @sophia.has_key?({tag: tags.first, oid: oid})
    end

    # looping (implemented):
    #      A          B          C
    #  1 (>=  0)  3 (>=  1) 10 (>=  3)
    # 11 (>= 10) 13 (>= 11) 14 (>= 13)
    # 14 (>= 14) 14 (>= 14) 14 (>= 14)
    # 17 (>  14) ...
    #
    # with backwards lookup:
    # A  B  C
    #  1
    #  1  3
    #  7  3
    #  7  7
    #  7  7 10
    # 11  7 10
    # 11 13 10
    # 14 13 10
    # 14 14 10
    # 14 14 14

    def find(tags : Array(String), limit : UInt64 = UInt64::MAX)
      last_oid = ""
      r = [] of String
      until r.size == limit
        cs = [] of DreamEnv::TagsCursor
        tags.each do |tag|
          c = @sophia.cursor(
            {tag: tag, oid: last_oid},
            if last_oid == (r.last rescue "")
              ">"
            else
              ">="
            end)
          return r unless c.next && c.data.not_nil![:tag] == tag
          last_oid = c.data.not_nil![:oid]
          cs << c
        end
        r << last_oid if cs.all? { |c| c.data.not_nil![:oid] == last_oid }
      end
      r
    end
  end
end
