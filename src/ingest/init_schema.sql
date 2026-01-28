create table if not exists users(
    user_id serial primary key,
    name text,
    email text,
    created_at timestamp default now()
);

create table if not exists products(
    product_id serial primary key,
    vendor text,
    vendor_product_id text,
    title text,
    category text,
    price numeric,
    currency text,
    created_at timestamp default now()
);

create table if not exists carts(
    cart_id serial primary key,
    user_id int references users(user_id),
    product_id int references products(product_id),
    qty int,
    added_at timestamp default now()
);