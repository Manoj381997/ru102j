package com.redislabs.university.RU102J.dao;

import com.redislabs.university.RU102J.api.Site;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.*;

public class SiteDaoRedisImpl implements SiteDao {
    private final JedisPool jedisPool;

    public SiteDaoRedisImpl(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    // When we insert a site, we set all of its values into a single hash.
    // We then store the site's id in a set for easy access.
    @Override
    public void insert(Site site) {
        try (Jedis jedis = jedisPool.getResource()) {
            String hashKey = RedisSchema.getSiteHashKey(site.getId());
            String siteIdKey = RedisSchema.getSiteIDsKey();
            jedis.hmset(hashKey, site.toMap());
            jedis.sadd(siteIdKey, hashKey);
        }
    }

    @Override
    public Site findById(long id) {
        try(Jedis jedis = jedisPool.getResource()) {
            String key = RedisSchema.getSiteHashKey(id);
            Map<String, String> fields = jedis.hgetAll(key);
            if (fields == null || fields.isEmpty()) {
                return null;
            } else {
                return new Site(fields);
            }
        }
    }

    // Challenge #1
    @Override
    public Set<Site> findAll() {
        // START Challenge #1
        try(Jedis jedis = jedisPool.getResource()) {
            Set<String> keys = jedis.smembers(RedisSchema.getSiteIDsKey());
            System.out.println("Length: "+ keys.size());
//            System.out.println(siteIds);
            Set<Site> sites = new HashSet<>();
            for (String key: keys) {
//                String siteId = key.substring(key.lastIndexOf(":") + 1);  // since the value is in this format "test:sites:info:3"
//                String hashKey = RedisSchema.getSiteHashKey(Long.parseLong(siteId));
//                System.out.println("Hash " + hashKey);
                Map<String, String> siteHash = jedis.hgetAll(key);
                if (siteHash != null && !siteHash.isEmpty())
                    sites.add(new Site(siteHash));
            }

            return sites;
        }
        // END Challenge #1
    }
}
