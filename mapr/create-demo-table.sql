CREATE DATABASE IF NOT EXISTS demodb;

USE demodb;

CREATE TABLE `users` (
  `id` int NOT NULL AUTO_INCREMENT,
  `title` text,
  `first` text,
  `last` text,
  `street` text,
  `city` text,
  `state` text,
  `postcode` text,
  `country` text,
  `gender` text,
  `email` text,
  `uuid` text,
  `username` text,
  `password` text,
  `phone` text,
  `cell` text,
  `dob` text,
  `registered` text,
  `large` text,
  `medium` text,
  `thumbnail` text,
  `nat` text,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;
