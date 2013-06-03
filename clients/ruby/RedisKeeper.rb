require 'redis'
require 'zk'

# Example.
# redis = RedisKeeper::Client.new()
# redis.set("key", "value")
# redis.get("key")

module RedisKeeper
    class Client
        def initialize (zk_servers = "127.0.0.1:2181", zk_server_path = '/redis/servers')
            @zk_servers = zk_servers
            @zk_server_path = zk_server_path

            selector() { |size, key|
                tmp_sum = 0
                key.each_byte do |i|
                    tmp_sum += i.ord
                end
                tmp_sum % size
            }

            @zk = ZK.new(zk_servers);
            @redis = Array.new
            refresh_redis()
        end

        def selector(&block)
             @selector = block
        end

        def method_missing(method, *args, &block)
            cluster_id = @selector.call( @redis.length, args.at(0) )
            @redis[cluster_id][0].send(method, *args)
        end

        def refresh_redis
            @zk.register(@zk_server_path+"/cluster") do |event|
                refresh_redis()
            end

            @zk.children(@zk_server_path+"/cluster", :watch => true).reverse_each { |child|
                child_path = @zk_server_path + "/cluster/" + child
                @zk.register(child_path) do |event|
                    tmp = @zk.get(child_path, :watch => true)
                    cluster_id = child_path.sub(/.*\//, "").to_i
                    if (tmp.at(0) != "0")
                        host = tmp.at(0).split(':')
                        redis_handle = Redis.new(:host => host.at(0), :port => host.at(1))
                        @redis[cluster_id] =  [redis_handle , host.at(0), host.at(1), child_path]
                    end
                end

                host = @zk.get(child_path, :watch => true).at(0).split(':')
                redis_handle = Redis.new(:host => host.at(0), :port => host.at(1))
                @redis[child.to_i] =  [redis_handle , host.at(0), host.at(1), child_path]
            }
        end
    end
end
