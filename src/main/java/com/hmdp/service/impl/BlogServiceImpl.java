package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.dto.ScrollResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@RequiredArgsConstructor
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {
    @Resource
    private IUserService userService;
    private final StringRedisTemplate redisTemplate;
    private final IFollowService followService;

    @Override
    public Result queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog -> {
            isBlogLiked(blog);
            queryBlogUser(blog);
        });
        return Result.ok(records);
    }

    private void queryBlogUser(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }

    @Override
    public Result queryBlogById(Long id) {
        Blog blog = getById(id);
        if (blog == null) {
            return Result.fail("笔记不存在！");
        }
        queryBlogUser(blog);
        isBlogLiked(blog);

        return Result.ok(getById(id));
    }

    private void isBlogLiked(Blog blog) {
        //1.获取用户信息
        if (UserHolder.getUser() != null) {
            Long userId = UserHolder.getUser().getId();
            String key = "blog:liked:" + blog.getId();
            //2.判断用户是否点过赞
            Double score = redisTemplate.opsForZSet().score(key, userId.toString());
            blog.setIsLike(score != null);
        }
    }

    @Override
    public Result likeBlog(Long id) {
        //1.获取用户信息
        Long userId = UserHolder.getUser().getId();
        String key = "blog:liked:" + id;
        //2.判断用户是否点过赞
        Double score = redisTemplate.opsForZSet().score(key, userId.toString());
        //2.1如果没点过赞，则数据库点赞数据加一
        if (score == null) {
            boolean isSuccess = update().setSql("liked = liked + 1").eq("id", id).update();
            if (isSuccess) {
                //如果数据库点赞数据成功更新，则保存数据到redis中
                redisTemplate.opsForZSet().add(key, userId.toString(), System.currentTimeMillis());
            }
        } else {
            //如果用户已经点过赞，则数据库点赞数据减一
            boolean isSuccess = update().setSql("liked = liked - 1").eq("id", id).update();
            if (isSuccess) {
                //如果数据库数据成功更新，则更改redis的用户集合
                redisTemplate.opsForZSet().remove(key, userId.toString());
            }
        }
        return Result.ok();
    }

    @Override
    public Result queryBlogLikes(Long id) {
        String key = "blog:liked:" + id;
        //1.查询出top5的用户id
        Set<String> top5 = redisTemplate.opsForZSet().range(key, 0, 5);
        if (top5 == null || top5.isEmpty()) {
            return Result.ok(Collections.emptyList());
        }
        //2.通过用户id得到用户
        //2.1先获取用户id
        List<Long> ids = top5.stream().map(Long::valueOf).collect(Collectors.toList());
        //2.2再获取用户
        List<UserDTO> dtoList = userService.listByIds(ids)
                .stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());

        //3.返回用户集合
        return Result.ok(dtoList);
    }

    @Override
    public Result saveBlog(Blog blog) {
        UserDTO user = UserHolder.getUser();
        blog.setUserId(user.getId());
        boolean isSuccess = save(blog);

        if (!isSuccess) {
            return Result.fail("新增笔记失败!");
        }
        List<Follow> followList = followService.query().eq("follow_user_id", user.getId()).list();
        for(Follow follow : followList) {
            String key = "feed:" + follow.getUserId();
            redisTemplate.opsForZSet().add(key, blog.getId().toString(), System.currentTimeMillis());
        }
        return Result.ok(blog.getId());
    }

    @Override
    public Result queryBlogOfFollow(Long max, Integer offset) {
        //1.查询当前用户
        UserDTO user = UserHolder.getUser();
        //2.获得收件箱
        String key = "feed:" + user.getId();
        Set<ZSetOperations.TypedTuple<String>> typedTuples = redisTemplate.opsForZSet()
                .rangeByScoreWithScores(key, 0, max, offset, 2);
        if(typedTuples == null || typedTuples.isEmpty()) {
            return Result.ok();
        }

        //4.解析时间戳
        long minTime = 0;
        int os = 1;
        List<Long> ids = new ArrayList<>(typedTuples.size());
        for (ZSetOperations.TypedTuple<String> typedTuple : typedTuples) {
            ids.add(Long.valueOf(typedTuple.getValue()));


            if(typedTuple.getScore() == minTime) {
                os++;
            } else {
                minTime = typedTuple.getScore().longValue();
                os = 1;
            }
        }
        //5.根据id查询出blogs
        String idStr = StrUtil.join(",", ids);
        List<Blog> blogs = query().in("id", ids)
                .last("ORDER BY FIELD(id," + idStr + ")")
                .list();
        for (Blog blog : blogs) {
            queryBlogUser(blog);
            isBlogLiked(blog);
        }

        ScrollResult scrollResult = new ScrollResult();
        scrollResult.setList(blogs);
        scrollResult.setMinTime(minTime);
        scrollResult.setOffset(os);

        return Result.ok(scrollResult);
    }
}
