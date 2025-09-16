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

    def after_initialize
      initialize @env
    end

    def initialize(@env : Env)
      @tc = (@env.cursor({i2ti: UInt32::MAX}, "<=").next.not_nil![:i2ti] rescue 0_u32) + 1
      @oc = (@env.cursor({i2oi: UInt32::MAX}, "<=").next.not_nil![:i2oi] rescue 0_u32) + 1
    end

    def add(object : Bytes, tags : Array(String))
      @env.transaction do |tx|
        oi = (@env[{o2io: object}]?.not_nil![:o2ii] rescue begin
          tx << {o2io: object, o2ii: @oc}
          tx << {i2oi: @oc, i2oo: object}
          @oc += 1
          @oc - 1
        end)
        tags.each do |tag|
          ti = (@env[{t2it: tag}]?.not_nil![:t2ii] rescue begin
            tx << {t2it: tag, t2ii: @tc}
            tx << {i2ti: @tc, i2tt: tag}
            @tc += 1
            @tc - 1
          end)
          tx << {t2ot: ti, t2oo: oi}
          tx << {o2to: oi, o2tt: ti}
          tx << {ti: ti, c: (@env[{ti: ti}]?.not_nil![:c] rescue 0_u32) + 1}
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
      @env.transaction do |tx|
        @env.from({o2to: oi, o2tt: 0_u32}) do |o2t|
          break unless o2t[:o2to] == oi
          ti = o2t[:o2tt]
          tx.delete({t2ot: ti, t2oo: oi})
          tx.delete({o2to: oi, o2tt: ti})
          tx << {ti: ti, c: (@env[{ti: ti}]?.not_nil![:c] - 1 rescue 0_u32)}
        end
        tx.delete({o2io: object})
        tx.delete({i2oi: oi})
      end
    end

    def delete(object : Bytes, tags : Array(String))
      oi = @env[{o2io: object}]?.not_nil![:o2ii] rescue return
      @env.transaction do |tx|
        tags.each do |t|
          ti = @env[{t2it: t}]?.not_nil![:t2ii] rescue next
          tx.delete({t2ot: ti, t2oo: oi})
          tx.delete({o2to: oi, o2tt: ti})
          tx << {ti: ti, c: (@env[{ti: ti}]?.not_nil![:c] - 1 rescue 0_u32)}
        end
      end
    end

    def find(present : Array(String), absent : Array(String) = [] of String, limit : UInt32 = UInt32::MAX, from : Bytes? = nil)
      fromi = if from
                @env[{o2io: from}]?.not_nil![:o2ii]
              else
                nil
              end

      ais = absent.compact_map { |t| @env[{t2it: t}]?.not_nil![:t2ii] rescue nil }
      ais.sort_by! { |ti| @env[{ti: ti}]?.not_nil![:c] }
      ais.reverse!

      r = [] of Bytes
      if present.size == 1
        ti = @env[{t2it: present.first}]?.not_nil![:t2ii] rescue return r
        @env.from({t2ot: ti, t2oo: (fromi.not_nil! rescue 0_u32)}, ">") do |t2o|
          break if r.size == limit || t2o[:t2ot] != ti
          r << @env[{i2oi: t2o[:t2oo]}]?.not_nil![:i2oo].clone if ais.all? { |ai| !@env.has_key?({t2ot: ai, t2oo: t2o[:t2oo]}) }
        end
        return r
      end

      pis = present.map { |t| @env[{t2it: t}]?.not_nil![:t2ii] rescue return r }
      pis.sort_by! { |ti| @env[{ti: ti}]?.not_nil![:c] }

      cs = [] of Dream::Env::T2oCursor

      i1 = 0
      i2 = 1
      loop do
        if cs.size == present.size && cs.all? { |c| c.data.not_nil![:t2oo] == cs.first.data.not_nil![:t2oo] }
          if ais.all? { |ai| !@env.has_key?({t2ot: ai, t2oo: cs.first.data.not_nil![:t2oo]}) }
            r << @env[{i2oi: cs.first.data.not_nil![:t2oo]}]?.not_nil![:i2oo].clone
            return r if r.size == limit
          end
          return r unless cs.first.next && cs.first.data.not_nil![:t2ot] == pis.first
          i1 = 0
          i2 = 1
        end

        if cs.size < present.size && cs.size <= i1
          if i1 == 0
            c = @env.cursor({t2ot: pis[i1], t2oo: (fromi.not_nil! rescue 0_u32)}, ">")
          else
            c = @env.cursor({t2ot: pis[i1], t2oo: cs.last.data.not_nil![:t2oo]})
          end
          return r unless c.next && c.data.not_nil![:t2ot] == pis[i1]
          cs << c
        end
        c1 = cs[i1]

        if cs.size < present.size && cs.size <= i2
          c = @env.cursor({t2ot: pis[i2], t2oo: cs.last.data.not_nil![:t2oo]})
          return r unless c.next && c.data.not_nil![:t2ot] == pis[i2]
          cs << c
        end
        c2 = cs[i2]

        until c2.data.not_nil![:t2oo] >= c1.data.not_nil![:t2oo]
          return r unless c2.next && c2.data.not_nil![:t2ot] == pis[i2]
        end
        if c2.data.not_nil![:t2oo] == c1.data.not_nil![:t2oo]
          i1 = (i1 + 1) % present.size
          i2 = (i2 + 1) % present.size
        else
          until cs.first.data.not_nil![:t2oo] >= cs[i2].data.not_nil![:t2oo]
            return r unless cs.first.next && cs.first.data.not_nil![:t2ot] == pis.first
          end
          i1 = 0
          i2 = 1
        end
      end
    end
  end
end
