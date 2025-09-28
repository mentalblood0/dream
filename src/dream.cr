require "yaml"

require "sophia"

module Dream
  Sophia.define_env Env, {t2o: {key: {t2ot: UInt32,
                                      t2oo: UInt32}},
                          o2t: {key: {o2to: UInt32,
                                      o2tt: UInt32}},
                          i2t: {key: {i2ti: UInt32},
                                value: {i2tt: String}},
                          t2i: {key: {t2it: String},
                                value: {t2ii: UInt32}},
                          i2o: {key: {i2oi: UInt32},
                                value: {i2oo: Bytes}},
                          o2i: {key: {o2io: Bytes},
                                value: {o2ii: UInt32}},
                          c: {key: {ti: UInt32},
                              value: {c: UInt32}}}

  class Index
    include YAML::Serializable
    include YAML::Serializable::Strict

    getter env : Env

    @[YAML::Field(ignore: true)]
    @tc : UInt32 = 0_u32
    @[YAML::Field(ignore: true)]
    @oc : UInt32 = 0_u32
    @[YAML::Field(ignore: true)]
    @intx = false

    def after_initialize
      @tc = (@env.cursor({i2ti: UInt32::MAX}, "<=").next.not_nil![:i2ti] rescue 0_u32) + 1
      @oc = (@env.cursor({i2oi: UInt32::MAX}, "<=").next.not_nil![:i2oi] rescue 0_u32) + 1
    end

    def initialize(@env : Env)
      after_initialize
    end

    protected def initialize(@env, @tc, @oc)
      @intx = true
    end

    def transaction(&)
      if @intx
        yield self
      else
        @env.transaction do |tx|
          r = Index.new tx, @tc, @oc
          yield r
        end
      end
    end

    def get_tag(prefix : String)
      @env.from({t2it: ""}) { |t2i| return t2i[:t2it].starts_with? prefix ? t2i[:t2it] : nil }
    end

    def objects(&)
      @env.from({o2io: Bytes.new 1}) { |o2i| yield o2i[:o2io].clone }
    end

    def objects
      r = [] of Bytes
      objects { |o| r << o }
      r
    end

    def add(object : Bytes, tags : Array(String))
      transaction do |tx|
        oi = (@env[{o2io: object}]?.not_nil![:o2ii] rescue begin
          tx.env << {o2io: object, o2ii: @oc}
          tx.env << {i2oi: @oc, i2oo: object}
          @oc += 1
          @oc - 1
        end)
        tags.each do |tag|
          ti = (@env[{t2it: tag}]?.not_nil![:t2ii] rescue begin
            tx.env << {t2it: tag, t2ii: @tc}
            tx.env << {i2ti: @tc, i2tt: tag}
            @tc += 1
            @tc - 1
          end)
          tx.env << {t2ot: ti, t2oo: oi}
          tx.env << {o2to: oi, o2tt: ti}
          tx.env << {ti: ti, c: (@env[{ti: ti}]?.not_nil![:c] rescue 0_u32) + 1}
        end
      end
    end

    def get(object : Bytes) : Array(String)
      r = [] of String
      oi = @env[{o2io: object}]?.not_nil![:o2ii] rescue return [] of String
      @env.from({o2to: oi, o2tt: 0_u32}) do |o2t|
        break unless o2t[:o2to] == oi
        ti = o2t[:o2tt]
        r << @env[{i2ti: ti}]?.not_nil![:i2tt]
      end
      r
    end

    def delete(object : Bytes)
      oi = @env[{o2io: object}]?.not_nil![:o2ii] rescue return
      transaction do |tx|
        @env.from({o2to: oi, o2tt: 0_u32}) do |o2t|
          break unless o2t[:o2to] == oi
          ti = o2t[:o2tt]
          tx.env.delete({t2ot: ti, t2oo: oi})
          tx.env.delete({o2to: oi, o2tt: ti})
          tx.env << {ti: ti, c: (@env[{ti: ti}]?.not_nil![:c] - 1 rescue 0_u32)}
        end
        tx.env.delete({o2io: object})
        tx.env.delete({i2oi: oi})
      end
    end

    def delete(object : Bytes, tags : Array(String))
      oi = @env[{o2io: object}]?.not_nil![:o2ii] rescue return
      transaction do |tx|
        tags.each do |t|
          ti = @env[{t2it: t}]?.not_nil![:t2ii] rescue next
          tx.env.delete({t2ot: ti, t2oo: oi})
          tx.env.delete({o2to: oi, o2tt: ti})
          tx.env << {ti: ti, c: (@env[{ti: ti}]?.not_nil![:c] - 1 rescue 0_u32)}
        end
      end
    end

    def find(present : Array(String), absent : Array(String) = [] of String, from : Bytes? = nil, &)
      fromi = if from
                @env[{o2io: from}]?.not_nil![:o2ii]
              else
                nil
              end

      ais = absent.compact_map { |t| @env[{t2it: t}]?.not_nil![:t2ii] rescue nil }
      ais.sort_by! { |ti| @env[{ti: ti}]?.not_nil![:c] }
      ais.reverse!

      if present.size == 1
        ti = @env[{t2it: present.first}]?.not_nil![:t2ii] rescue return
        @env.from({t2ot: ti, t2oo: (fromi.not_nil! rescue 0_u32)}, ">") do |t2o|
          break if t2o[:t2ot] != ti
          yield @env[{i2oi: t2o[:t2oo]}]?.not_nil![:i2oo].clone if ais.all? { |ai| !@env.has_key?({t2ot: ai, t2oo: t2o[:t2oo]}) }
        end
        return
      end

      pis = present.map { |t| @env[{t2it: t}]?.not_nil![:t2ii] rescue return }
      pis.sort_by! { |ti| @env[{ti: ti}]?.not_nil![:c] }

      cs = [] of Dream::Env::T2oCursor

      i1 = 0
      i2 = 1
      loop do
        if cs.size == present.size && cs.all? { |c| c.data.not_nil![:t2oo] == cs.first.data.not_nil![:t2oo] }
          if ais.all? { |ai| !@env.has_key?({t2ot: ai, t2oo: cs.first.data.not_nil![:t2oo]}) }
            yield @env[{i2oi: cs.first.data.not_nil![:t2oo]}]?.not_nil![:i2oo].clone
          end
          return unless cs.first.next && cs.first.data.not_nil![:t2ot] == pis.first
          i1 = 0
          i2 = 1
        end

        if cs.size < present.size && cs.size <= i1
          if i1 == 0
            c = @env.cursor({t2ot: pis[i1], t2oo: (fromi.not_nil! rescue 0_u32)}, ">")
          else
            c = @env.cursor({t2ot: pis[i1], t2oo: cs.last.data.not_nil![:t2oo]})
          end
          return unless c.next && c.data.not_nil![:t2ot] == pis[i1]
          cs << c
        end
        c1 = cs[i1]

        if cs.size < present.size && cs.size <= i2
          c = @env.cursor({t2ot: pis[i2], t2oo: cs.last.data.not_nil![:t2oo]})
          return unless c.next && c.data.not_nil![:t2ot] == pis[i2]
          cs << c
        end
        c2 = cs[i2]

        until c2.data.not_nil![:t2oo] >= c1.data.not_nil![:t2oo]
          return unless c2.next && c2.data.not_nil![:t2ot] == pis[i2]
        end
        if c2.data.not_nil![:t2oo] == c1.data.not_nil![:t2oo]
          i1 = (i1 + 1) % present.size
          i2 = (i2 + 1) % present.size
        else
          until cs.first.data.not_nil![:t2oo] >= cs[i2].data.not_nil![:t2oo]
            return unless cs.first.next && cs.first.data.not_nil![:t2ot] == pis.first
          end
          i1 = 0
          i2 = 1
        end
      end
    end

    def find(present : Array(String), absent : Array(String) = [] of String, limit : UInt32 = UInt32::MAX, from : Bytes? = nil)
      r = [] of Bytes
      find(present, absent, from) do |o|
        break if r.size == limit
        r << o
      end
      r
    end
  end
end
